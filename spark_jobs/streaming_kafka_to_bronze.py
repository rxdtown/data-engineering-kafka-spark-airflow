from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaToBronze").getOrCreate()

schema = (StructType()
    .add("id_commande", IntegerType())
    .add("produit", StringType())
    .add("montant", DoubleType())
    .add("ville", StringType())
    .add("timestamp", StringType())
)

df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "commandes")
    .option("startingOffsets", "earliest")
    .load()
)

df_parsed = (df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

query = (df_parsed.writeStream
    .format("parquet")
    .option("path", "/opt/data/bronze/commandes")
    .option("checkpointLocation", "/opt/data/checkpoints/bronze_commandes")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()