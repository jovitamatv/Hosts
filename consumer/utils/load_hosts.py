from delta.tables import DeltaTable
from pyspark.sql.types import StringType, TimestampType
import pyspark.sql.functions as F


class HostsLoader:
    """
    Class to handle loading transformed host data into a Delta Lake table.
    And optimize the table for performance.
    
    Attributes:
        spark (SparkSession): The Spark session.
        delta_table_path (str): The path to the Delta Lake table.
        logger (logging.Logger): Logger instance for logging.
    """

    def __init__(self, spark, delta_table_path, logger):
        """
        Initialize the HostsLoader with Spark session, Delta table path, and logger.
        
        Args:
            spark (SparkSession): The Spark session.
            delta_table_path (str): The path to the Delta Lake table.
            logger (logging.Logger): Logger instance for logging.
        """
        self.spark = spark
        self.delta_table_path = delta_table_path
        self.logger = logger

    def create_delta_table(self):
        """Create a Delta Lake table if it does not exist."""
        
        delta_table = (
            DeltaTable.create(self.spark)
            .tableName("hosts")
            .addColumn("defanged_host", StringType(), nullable=False)
            .addColumn("hashed_host", StringType(), nullable=False)
            .addColumn("category", StringType())
            .addColumn("source_name", StringType(), nullable=False)
            .addColumn("source_url", StringType(), nullable=False)
            .addColumn("retrieved_at", TimestampType(), nullable=False)
            .location(self.delta_table_path)
            .execute()
        )

    def check_table_exists(self):
        """
        Check if the Delta Lake table exists at the specified path.

        Returns:
            bool: True if the table exists, False otherwise.
        """

        if DeltaTable.isDeltaTable(self.spark, self.delta_table_path):
            return True
        else:
            self.logger.info(f"Delta table does not exist at {self.delta_table_path}")
            return False

    def write_to_deltalake(self, batch):
        """
        Write a batch of data to a Delta Lake table.
        
        Args:
            batch (list): List of dictionaries representing the batch of data.
        """

        spark_df =(self.spark.createDataFrame(batch)
                    .withColumn("retrieved_at", F.to_timestamp("retrieved_at"))
                    .dropDuplicates(['hashed_host']))

        if not self.check_table_exists():
            self.logger.info(f"Creating Delta table at {self.delta_table_path}")
            self.create_delta_table()

        delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)


        (delta_table.alias("target")
            .merge(
            spark_df.alias("source"),
            "target.hashed_host = source.hashed_host")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute()
        )
        delta_table.optimize().executeCompaction()
        spark_df.unpersist()
        self.logger.info(f"Successfully wrote batch size of {len(batch)} to Delta table at {self.delta_table_path}")

            

    def optimize_delta_table(self):
        """
        Optimize a Delta Lake table to improve performance.
        Includes compaction and vacuuming.
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
            delta_table.vacuum(7 * 24)
            delta_table.optimize().executeZOrderBy("hashed_host")
            self.logger.info(f"Optimized Delta table at {self.delta_table_path}")
        except Exception as e:
            self.logger.error(f"Failed to optimize Delta table: {e}", exc_info=True)