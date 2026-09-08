from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

df_silver = spark.read.parquet("/opt/data/silver/commandes")

df_gold = (df_silver
    .groupBy("ville", "jour")
    .agg(
        F.sum("montant").alias("volume_total"),
        F.count("*").alias("nb_commandes")
    )
)

(df_gold.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/analytics")
    .option("dbtable", "reporting_commandes")
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)
print(f"Gold : {df_gold.count()} lignes écrites dans PostgreSQL")