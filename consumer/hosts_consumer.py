import json
import os
import time
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from utils.logger import setup_logger
from consumer.utils.kafka_utils import create_costumer
from consumer.utils.transform_hosts import HostsTransformer
from consumer.utils.load_hosts import HostsLoader

logger = setup_logger("hosts_consumer", 
                      f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_consumer.log")
status_logger = setup_logger("hosts_consumer_status",
                             f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_consumer_status.log",
                             console=False)

idle_count = 0
max_idle = int(os.getenv('MAX_IDLE_COUNT'))
batch = []
batch_size = int(os.getenv('LOADER_BATCH_SIZE'))
delta_table_path = os.getenv('DELTA_TABLE_PATH')

builder = (
    SparkSession.builder
    .appName("HostsConsumer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

consumer = create_costumer(logger)

messages_processed = 0
batches_written = 0
failed_messages = 0
failed_transformations = 0
start_time = time.time()

status_logger.info(f"STATUS: starting | batch_size={batch_size} | max_idle={max_idle}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            idle_count += 1
            if idle_count >= max_idle:
                logger.info("No new messages, exiting consumer loop.")
                break
            continue
        idle_count = 0

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}", exc_info=True)
            status_logger.error(f"STATUS: failed | error={msg.error()}")
            failed_messages += 1
            continue

        try:
            message_value = msg.value().decode('utf-8')
            message = json.loads(message_value)

            transformer = HostsTransformer(message, logger)
            transformed_record = transformer.transform_hosts()
            if transformed_record is None:
                failed_transformations += 1
                status_logger.error(
                    f"STATUS: failed | message={message_value} | "
                    f"error=transformation_failed"
                )
                continue
            batch.append(transformed_record)

            if len(batch) >= batch_size:
                try:
                    loader = HostsLoader(spark, batch, delta_table_path, logger)
                    loader.write_to_deltalake()
                    logger.info(f"Wrote batch {batches_written} of size {len(batch)} to Delta Lake.")
                    status_logger.info(
                        f"STATUS: processing | batch_num={batches_written} | "               
                        f"batch_size={len(batch)}  | progress={messages_processed}"
                    )
                    batches_written += 1
                    messages_processed += len(batch)
                except Exception as e:
                    logger.error(f"Failed to write batch to Delta Lake: {e}", exc_info=True)
                    status_logger.error(f"STATUS: failed | error={e} | batch_num={batches_written}")
                    failed_messages += len(batch)
                batch.clear()

        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.error(f"Failed to decode JSON message {msg.value()}: {e}", exc_info=True)
            status_logger.error(f"STATUS: failed | error={e} | message={msg.value()}")
            failed_messages += 1
            continue

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user.")
    status_logger.info(
        f"STATUS: interrupted | messages_processed={messages_processed} | "
        f"failed={failed_messages}"
    )
finally:
    loader = HostsLoader(spark, batch, delta_table_path, logger)
    if batch:
        batches_written += 1
        messages_processed += len(batch)
        loader.write_to_deltalake()
        
    loader.optimize_delta_table()

    duration = time.time() - start_time
    if failed_messages > 0 or failed_transformations > 0:
        status_logger.error(
            f"STATUS: completed_with_errors | messages_processed={messages_processed} | "
            f"failed_msgs={failed_messages} | failed_transformations={failed_transformations} | "
            f"duration={duration:.2f}s"
        )
    else:
        status_logger.info(
                f"STATUS: completed | messages_processed={messages_processed} | "
                f"failed_msgs={failed_messages} | failed_transformations={failed_transformations} | "
                f"duration={duration:.2f}s"
        )

    consumer.close()
    logger.info("Consumer closed.")
    spark.stop()
    logger.info("Spark session stopped.")