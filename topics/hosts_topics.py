import yaml
import os
import time
from confluent_kafka.admin import AdminClient
from utils.logger import setup_logger
from topics.utils.topics_utils import build_topics, create_topics

logger = setup_logger("hosts_topics", 
                      f"{os.getenv('PROCESS_LOG_DIRS')}/hosts_topics.log")

def main():
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)

    start_time = time.time()

    topics = build_topics(cfg, os.getenv('RETENTION_MS'))
    admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})
    created_topics, existing_topics, failed_topics = create_topics(admin, topics, logger)
    
    duration = time.time() - start_time

    if failed_topics == 0:
        logger.info(
            f"STATUS: completed | created={created_topics} | "
            f"existing={existing_topics} | failed={failed_topics} |"
            f" duration={duration:.2f}s"
        )
    else:
        logger.error(
            f"STATUS: completed_with_errors | created={created_topics} | "
            f"existing={existing_topics} | failed={failed_topics} | "
            f"duration={duration:.2f}s")
        
if __name__ == "__main__":
    main()