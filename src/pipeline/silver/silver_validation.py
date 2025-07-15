import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

class SilverValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_and_quarantine(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        current_year = F.year(F.current_date())
        invalid_conditions = (
            (F.col("construction_year").isNull()) |
            (F.col("construction_year") < 1960) |
            (F.col("construction_year") > current_year) |
            (F.col("sale_price_eur").isNull()) | (F.col("sale_price_eur") <= 0) |
            (F.col("price_per_sqm_eur").isNull()) | (F.col("price_per_sqm_eur") <= 0) |
            F.col("property_id").isNull() |
            (F.col("size_sqm") <= 0)
        )
        invalid_df = df.filter(invalid_conditions).withColumn("is_quarantined", F.lit(True))
        valid_df = df.filter(~invalid_conditions)

        quarantine_reason = (
            F.when(F.col("construction_year").isNull(), "Missing construction year")
            .when(F.col("construction_year") < 1960, "Construction year too old")
            .when(F.col("construction_year") > current_year, "Construction year in the future")
            .when((F.col("sale_price_eur").isNull()) | (F.col("sale_price_eur") <= 0), "Invalid or missing sale price")
            .when((F.col("price_per_sqm_eur").isNull()) | (F.col("price_per_sqm_eur") <= 0), "Invalid or missing price per sqm")
            .when(F.col("property_id").isNull(), "Missing property ID")
            .when(F.col("size_sqm") <= 0, "Invalid size")
            .otherwise("Unknown reason")
        )
        return valid_df, invalid_df.withColumn("quarantine_reason", quarantine_reason)