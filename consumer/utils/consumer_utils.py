import os
import re
import time
import json
from confluent_kafka import Consumer
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from consumer.utils.transform_hosts import HostsTransformer
from consumer.utils.load_hosts import HostsLoader

def create_costumer(logger, retries=3, wait_time=5):
    """Create and return a Kafka consumer subscribed to relevant topics.

    Args:
        logger (Logger): Logger instance for logging information and errors.
        retries (int): Number of retries for creating the consumer.
        wait_time (int): Wait time in seconds between retries.

    Returns:
        Consumer: Configured Kafka consumer instance.
    """

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id": os.getenv("KAFKA_GROUP_ID"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

    for attempt in range(retries):
        try:
            metadata = consumer.list_topics(timeout=5)
            topics = list(metadata.topics.keys())
            matched_topics = [topic for topic in topics if re.match("^hosts\\.raw\\..*", topic)]
            consumer.subscribe(matched_topics)
            logger.info(f"Subscribed to topics: {matched_topics}")
            return consumer
        except Exception as e:
            logger.error(
                f"Error connecting to Kafka: {e}"
                f"Attempt {attempt + 1} of {retries}. Retrying in {wait_time} seconds."
            , exc_info=True)
            time.sleep(wait_time)
    logger.error("STATUS: failed | error=exceeded maximum retries, could not connect to Kafka")
    raise Exception("Could not connect to Kafka after retries.")

def init_spark(logger):
    """
    Initialize and return a Spark session configured for Delta Lake.

    Args:
        logger (Logger): Logger instance for logging information and errors.

    Returns:
        SparkSession: Configured Spark session instance.
    """

    builder = (
        SparkSession.builder
        .appName("HostsConsumer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"STATUS: failed | error={e} | message={e}", exc_info=True)
        raise

def _decode_and_transform_message(msg, logger):
    """
    Decode a Kafka message and transform it using HostsTransformer.

    Args:
        msg (Message): Kafka message instance.
        logger (Logger): Logger instance for logging information and errors.

    Returns:
        dict or None: Transformed record or None if transformation (decoding) failed.
    """

    try:
        message_value = msg.value().decode('utf-8')
        message = json.loads(message_value)

        transformer = HostsTransformer(message, logger)
        transformed_record = transformer.transform_hosts()
        return transformed_record
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        logger.error(f"STATUS: failed | error={e} | message={msg.value()}", exc_info=True)
        return None

def _flush_batch_to_delta(loader, batch, logger, 
                          batches_written, messages_processed, failed_messages):
    """
    Flush the current batch of transformed records to Delta Lake.

    Args:
        loader (HostsLoader): Instance of HostsLoader to handle Delta Lake operations.
        batch (list): List of transformed records.
        logger (Logger): Logger instance for logging information and errors.
        batches_written (int): Number of batches written so far.
        messages_processed (int): Total number of messages processed so far.
        failed_messages (int): Total number of failed messages so far.

    Returns:
        tuple: Tuple containing:
            - batches_written (int): Updated number of batches written.
            - messages_processed (int): Updated total number of messages processed.
            - failed_messages (int): Updated total number of failed messages.
    """

    try:
        loader.write_to_deltalake(batch)
        messages_processed += len(batch)
        batches_written += 1
        logger.info(
            f"STATUS: processing | batch_num={batches_written} | "               
            f"batch_size={len(batch)}  | progress={messages_processed}"
        )
    except Exception as e:
        failed_messages += len(batch)
        logger.error(f"STATUS: failed | error={e} | batch_num={batches_written + 1}", exc_info=True)
    return batches_written, messages_processed, failed_messages

def process_messages(consumer, spark, delta_table_path, logger, batch_size, max_idle):
    """
    Fetch messages from Kafka, transform them, and write to Delta Lake in batches.

    Args:
        consumer (Consumer): Kafka consumer instance.
        spark (SparkSession): Spark session instance.
        delta_table_path (str): Path to the Delta Lake table.
        logger (Logger): Logger instance.
        batch_size (int): Number of messages to process in each batch.
        max_idle (int): Maximum number of consecutive idle polls before exiting.

    Returns:
        tuple: Tuple containing:
            - messages_processed (int): Total number of successfully processed messages.
            - failed_messages (int): Total number of messages that failed processing.
            - failed_transformations (int): Total number of messages that failed transformation.
    """

    logger.info(f"STATUS: starting | batch_size={batch_size} | max_idle={max_idle}")

    batch = []
    idle_count = 0
    messages_processed = 0
    batches_written = 0
    failed_messages = 0
    failed_transformations = 0

    loader = HostsLoader(spark, delta_table_path, logger)

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
            logger.error(f"STATUS: failed | error={msg.error()}")
            failed_messages += 1
            continue

        transformed_record = _decode_and_transform_message(msg, logger)
        
        if transformed_record is None:
            failed_transformations += 1
            continue

        batch.append(transformed_record)

        if len(batch) >= batch_size:
            batches_written, messages_processed, failed_messages = _flush_batch_to_delta(
                loader, batch, logger, batches_written, 
                messages_processed, failed_messages
            )
            batch.clear()

    if batch:
        batches_written, messages_processed, failed_messages = _flush_batch_to_delta(
            loader, batch, logger, batches_written, 
            messages_processed, failed_messages
        )
    
    loader.optimize_delta_table()
    
    return messages_processed, failed_messages, failed_transformations