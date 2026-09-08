from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

df = spark.read.parquet("/opt/data/bronze/commandes")

df_silver = (df
    .dropDuplicates(["id_commande"])
    .filter(F.col("montant") > 0)
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("jour", F.to_date("timestamp"))
)

df_silver.write.mode("overwrite").parquet("/opt/data/silver/commandes")
print(f"Silver : {df_silver.count()} lignes écrites")