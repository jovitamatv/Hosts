import yaml
import json
import os
import time
from confluent_kafka import Producer
from utils.logger import setup_logger
from producer.utils.extract_hosts import HostsExtractor

logger = setup_logger("hosts_producer", 
                      f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_producer.log")
status_logger = setup_logger("hosts_producer_status",
                             f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_producer_status.log",
                             console=False)

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

total_sources = len(cfg['sources'])
total_hosts_sent = 0
total_hosts_failed = 0
failed_sources = 0
start_time = time.time()

status_logger.info(f"STATUS: starting | sources_to_process={len(cfg['sources'])}")


for source_idx, source in enumerate(cfg['sources']):
    try:
        topic_name = f"hosts.raw.{source['name']}"
        source_info = {
            "name": source['name'],
            "url": source.get('url'),
            "type": source.get('type'),
            "category": source.get('category')
        }

        status_logger.info(
            f"STATUS: processing | source={source['name']} | "
            f"progress={source_idx + 1}/{total_sources}"
        )

        extractor = HostsExtractor(source_info, logger)
        hosts, retrieval_time = extractor.extract_hosts()
        if not hosts:
            status_logger.error(
                f"STATUS: failed | source={source['name']} | "
                f"error=No hosts extracted"
            )
            failed_sources += 1
            continue
        
    except Exception as e:
        logger.error(f"Failed to extract hosts for source {source['name']}: {e}", exc_info=True)
        status_logger.error(f"STATUS: failed | source={source['name']} | error={e}")
        failed_sources += 1
        continue
    
    failed_hosts = 0
    for i, host in enumerate(hosts):
        try:
            message = {
                "name": source_info["name"],
                "url": source_info["url"],
                "category": source_info["category"],
                "retrieval_time": retrieval_time,
                "host": host
            }
            producer.produce(topic_name, value=json.dumps(message).encode("utf-8"))
            producer.poll(0)

            if i % batch_size == 0 and i > 0:
                producer.flush()
                logger.info(f"Flushed {i} hosts to topic {topic_name}")
        except Exception as e:
            logger.error(f"Failed to produce message for host {host}: {e}", exc_info=True) 
            status_logger.error(
                f"STATUS: failed | source={source['name']} | "
                f"host={host} | error={e}"
            )
            failed_hosts += 1
            continue

    total_hosts_sent += (len(hosts) - failed_hosts)
    total_hosts_failed += failed_hosts
    
producer.flush()
logger.info("Hosts sent to Kafka.")

duration = time.time() - start_time

if total_hosts_failed > 0 or failed_sources > 0:
    status_logger.error(
        f"STATUS: completed_with_errors | total_hosts_sent={total_hosts_sent} | "
        f"total_hosts_failed={total_hosts_failed} | "
        f"failed_sources={failed_sources} | duration={duration:.2f}s"
    )
else:
    status_logger.info(
        f"STATUS: completed | total_hosts_sent={total_hosts_sent} | "
        f"total_hosts_failed={total_hosts_failed} | "
        f"failed_sources={failed_sources} | duration={duration:.2f}s"
    )