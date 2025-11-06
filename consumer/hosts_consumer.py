import json
import os
import time
from utils.logger import setup_logger
from consumer.utils.consumer_utils import create_costumer, init_spark, process_messages

logger = setup_logger("hosts_consumer", 
                      f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_consumer.log")

def main():
    spark = init_spark(logger)
    consumer = create_costumer(logger)
    max_idle = int(os.getenv('MAX_IDLE_COUNT'))
    batch_size = int(os.getenv('LOADER_BATCH_SIZE'))
    delta_table_path = os.getenv('DELTA_TABLE_PATH')
    try:
        start_time = time.time()
        messages_processed, failed_messages, failed_transformations = process_messages(
            consumer, spark, delta_table_path, logger, batch_size, max_idle
        )
    except KeyboardInterrupt:
        logger.info(
            f"STATUS: interrupted by user| messages_processed={messages_processed} | "
            f"failed={failed_messages}"
        )
    finally:
        duration = time.time() - start_time
        if failed_messages > 0 or failed_transformations > 0:
            logger.error(
                f"STATUS: completed_with_errors | messages_processed={messages_processed} | "
                f"failed_msgs={failed_messages} | failed_transformations={failed_transformations} | "
                f"duration={duration:.2f}s"
            )
        else:
            logger.info(
                    f"STATUS: completed | messages_processed={messages_processed} | "
                    f"failed_msgs={failed_messages} | failed_transformations={failed_transformations} | "
                    f"duration={duration:.2f}s"
            )

        consumer.close()
        logger.info("Consumer closed.")
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()