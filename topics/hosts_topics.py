import yaml
import os
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from utils.logger import setup_logger

logger = setup_logger("hosts_topics", 
                      f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_topics_creator.log")
status_logger = setup_logger("hosts_topics_status",
                             f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_topics_status.log",
                             console=False)

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})

topics = []
created_topics = 0
existing_topics = 0
failed_topics = 0
start_time = time.time()

for source in cfg['sources']:
    topic_name = f"hosts.raw.{source['name']}"
    partitions = source.get('topic_partitions', 1)
    replication = source.get('topic_replication', 1)
    topics.append(
        NewTopic(
            topic_name, 
            num_partitions=partitions, 
            replication_factor=replication,
            config={
                'retention.ms': os.getenv('RETENTION_MS')
            }
        )
    )

status_logger.info(f"STATUS: starting | topics_to_create={len(topics)}")

logger.info(f"Topics creation initiated")
fs = admin.create_topics(topics)

for topic, f in fs.items():
    try:
        f.result()
        logger.info(f"Created topic {topic}")
        status_logger.info(f"STATUS: created | topic={topic}")
        created_topics += 1
    except KafkaException as e:
        if e.args[0].code() == 36:
            logger.info(f"Topic {topic} already exists")
            status_logger.info(f"STATUS: exists | topic={topic}")
            existing_topics += 1
    except Exception as e:
        logger.error(f"Failed to create topic {topic}: {e}", exec_info=True)
        status_logger.error(f"STATUS: failed | topic={topic} | error={e}")
        failed_topics += 1

duration = time.time() - start_time
if failed_topics == 0:
    status_logger.info(
        f"STATUS: completed | created={created_topics} | "
        f"existing={existing_topics} | failed={failed_topics} |"
        f" duration={duration:.2f}s"
    )
else:
    status_logger.error(
        f"STATUS: completed_with_errors | created={created_topics} | "
        f"existing={existing_topics} | failed={failed_topics} | "
        f"duration={duration:.2f}s")