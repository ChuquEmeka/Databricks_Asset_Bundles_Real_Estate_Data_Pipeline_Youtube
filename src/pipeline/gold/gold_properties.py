import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class GoldProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def aggregate_property_valuations(self, fact_df: DataFrame, dim_df: DataFrame) -> DataFrame:
        fact = fact_df.alias("fact")
        dim = dim_df.alias("dim")

        joined_df = fact.join(dim, fact["property_id"] == dim["property_id"])

        return joined_df.groupBy(dim["borough"]).agg(
            F.round(F.avg(fact["sale_price_eur"]), 2).alias("avg_sale_price_eur"),
            F.round(F.avg(fact["transaction_price_per_sqm_eur"]), 2).alias("avg_price_per_sqm_eur")
        )

    def build_neighborhood_trends(self, dim_df: DataFrame) -> DataFrame:
        return (
            dim_df.groupBy("borough")
            .agg(
                F.round(F.avg("median_income_eur"), 2).alias("avg_median_income_eur"),
                F.round(F.avg("population_density_per_sqm"), 2).alias("avg_population_density_per_sqm"),
                F.round(F.avg("crime_rate_per_1000"), 2).alias("avg_crime_rate_per_1000"),
                F.round(F.max("avg_price_per_sqm_2025_eur"), 2).alias("max_avg_price_per_sqm_2025_eur")
            )
        )