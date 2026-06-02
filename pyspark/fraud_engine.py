from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

landing_path = str(BASE_DIR / "data" / "landing" / "*.json")

spark = SparkSession.builder \
    .appName("FraudEngine") \
    .getOrCreate()

df = spark.read.json(landing_path)

fraud_df = df.withColumn(
    "fraud_score",
    when(col("failed_login_attempts") > 5, 20).otherwise(0)
    + when(col("is_new_device") == True, 20).otherwise(0)
    + when(col("amount") > col("monthly_income"), 30).otherwise(0)
    + when(col("recent_profile_change") == True, 15).otherwise(0)
    + when(col("new_beneficiary") == True, 15).otherwise(0)
)

flagged_df = fraud_df.filter(col("fraud_score") >= 50)

clean_df = fraud_df.filter(col("fraud_score") < 50)

fraud_output = str(BASE_DIR / "data" / "fraud")
clean_output = str(BASE_DIR / "data" / "clean")

flagged_df.write.mode("overwrite").json(fraud_output)

clean_df.write.mode("overwrite").json(clean_output)

print("Fraud records:", flagged_df.count())
print("Clean records:", clean_df.count())

spark.stop()