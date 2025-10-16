from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("HostsConsumer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("data/delta")
df.createOrReplaceTempView("hosts")

spark.sql("SELECT COUNT(*) as total FROM hosts").show()
spark.sql("SELECT category, COUNT(*) as count FROM hosts GROUP BY category").show()
spark.sql("SELECT * FROM hosts WHERE category = 'Malware' LIMIT 10").show()
spark.sql("SELECT * FROM hosts WHERE defanged_host = 'www[.]yuechengcn[.]com'").show()
spark.sql("SELECT * FROM hosts WHERE hashed_host = SHA2('www.yuechengcn.com', 256)").show()

spark.stop()
