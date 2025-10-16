# Hosts Data Processing System

A distributed system for collecting, processing, and analyzing hosts/blocklist data from various sources using Apache Kafka, Apache Spark, and Delta Lake.

### Starting the System

The system is designed to run in Docker containers. To start the entire pipeline:

```bash


# Start all services and follow the monitor logs
docker-compose up -d && docker-compose logs -f monitor

# Start all services without running the monitor logs in the console
docker-compose up -d
```


This will start the following services:
- **Kafka**: Message broker for data streaming
- **Hosts Topics**: Creates Kafka topics for each data source based on config.yaml
- **Hosts Producer**: Fetches data from configured sources and publishes to Kafka
- **Hosts Consumer**: Consumes data from Kafka, transforms and stores in Delta Lake format
- **Monitor**: Monitors the health of all services

### Stopping the System

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (removes all data)
docker-compose down -v
```

## Project Structure

```
.
├── config.yaml              # Data source configurations
├── docker-compose.yaml      # Docker services orchestration
├── requirements.txt         # Python dependencies
├── requirements.in          # Source dependencies
├── consumer/               # Kafka consumer and data processing
│   ├── hosts_consumer.py   # Main consumer application
│   └── utils/              # Consumer utilities
├── producer/               # Data fetching and Kafka producer
│   ├── hosts_producer.py   # Main producer application
│   └── utils/              # Producer utilities
├── topics/                 # Kafka topic management
│   └── hosts_topics.py     # Topic creation and configuration
├── monitoring/             # System monitoring
│   └── monitor.py          # Health monitoring service
├── utils/                  # Shared utilities
│   ├── query_examples.py   # SQL query examples
│   ├── logger.py           # Logging configuration
│   └── email_notifier.py   # Email notifications
├── data/                   # Data storage
│   ├── delta/              # Delta Lake tables
│   ├── kafka/              # Kafka logs
│   └── logs/               # Application logs
└── examples/               # Sample data files
```


## Technology Stack

- Docker Compose: Reproducible, orchestration for all services and local volumes.
- Apache Kafka (KRaft) + Confluent Kafka (Python): Reliable, scalable data streaming; one topic per source, partitions enable parallel processing; fast and stable producer/consumer clients.
- Apache Spark (PySpark): Scalable processing for parsing, transforming, and aggregating large datasets.
- Delta Lake (delta-spark): schema evolution, compaction, data versioning.

## Querying Data

Once the system is running and data has been processed, you can query the Delta Lake table.

### Using the Query Examples

Before running the query examples, create and activate a virtual environment and install the dependencies from requirements.txt.

```bash
# Run example queries
python utils/query_examples.py
```

## Configuration

### Data Sources (`config.yaml`)

Add new data sources by editing the configuration:

```yaml
sources:
  - name: your-source-name            # Unique, human-readable identifier
    url: https://example.com/hosts    # HTTP(S) URL or local file path
    type: http                        # Source type (e.g., http, file)
    category: Malware                 # Logical grouping for analytics
    format: hosts                     # Input format (e.g., hosts, txt, csv)
    topic_partitions: 3               # Kafka partitions for the source topic
    topic_replication_factor: 1       # Kafka replication factor for the topic
```

### Environment Variables

Key environment variables for configuration:


| Variable | Description | Default |
|----------|-------------|---------|
| `PYTHONPATH` | Python import path inside containers | `/app` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `kafka:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID for the hosts-consumer | `hosts-consumer-group` |
| `PRODUCER_BATCH_SIZE` | Number of records to batch per send in producer | `1000` |
| `LOADER_BATCH_SIZE` | Number of records loaded per batch by consumer | `100000` |
| `MAX_IDLE_COUNT` | Max consecutive empty polls before consumer exits | `10` |
| `DELTA_TABLE_PATH` | Delta Lake storage path (inside container) | `/app/data/delta` |
| `PROCESS_LOG_DIRS` | Directory for service/process logs | `/app/data/logs` |
| `RETENTION_MS` | Kafka topic retention (ms) applied by topics service | `600000` |
| `MONITOR_SERVICES` | Comma-separated service:status_log mapping for monitor | `hosts-topics:hosts_topics_status.log, hosts-producer:hosts_producer_status.log, hosts-consumer:hosts_consumer_status.log` |



### Monitoring

A separate monitoring service (monitoring/monitor.py) scans the service status log files to gather information. It highlights failures and can send email alerts when errors are detected.

Useful commands:
- View monitor output: docker-compose logs -f monitor

Alerts:
- When a status line contains "STATUS: completed_with_errors", the monitor calls utils/email_notifier.send_email.
  Configure your email settings there (and required env vars) to enable notifications.


