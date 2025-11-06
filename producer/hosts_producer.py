import yaml
import json
import os
import time
from confluent_kafka import Producer
from utils.logger import setup_logger
from producer.utils.producer_utils import produce_messages

logger = setup_logger("hosts_producer", 
                      f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_producer.log")

def main():
    batch_size = int(os.getenv("PRODUCER_BATCH_SIZE"))

    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)

    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "batch.size": 65536,
        "linger.ms": 100,
        "compression.type": "snappy",
        "acks": 1,
        "retries": 3
    })

    start_time = time.time()

    total_hosts_sent, total_hosts_failed, failed_sources = produce_messages(
        cfg, producer, logger, batch_size
    )

    duration = time.time() - start_time

    if total_hosts_failed > 0 or failed_sources > 0:
        logger.error(
            f"STATUS: completed_with_errors | total_hosts_sent={total_hosts_sent} | "
            f"total_hosts_failed={total_hosts_failed} | "
            f"failed_sources={failed_sources} | duration={duration:.2f}s"
        )
    else:
        logger.info(
            f"STATUS: completed | total_hosts_sent={total_hosts_sent} | "
            f"total_hosts_failed={total_hosts_failed} | "
            f"failed_sources={failed_sources} | duration={duration:.2f}s"
        )

if __name__ == "__main__":
    main()
    