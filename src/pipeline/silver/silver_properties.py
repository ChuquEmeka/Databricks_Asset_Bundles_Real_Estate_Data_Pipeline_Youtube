import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from silver_validation import SilverValidator
from silver_schema import PropertySchema

class SilverProcessor:
    def __init__(self, spark: SparkSession, bronze_table: str):
        self.spark = spark
        self.bronze_table = bronze_table
        self.validator = SilverValidator(spark)

    def flatten_property_data(self, df: DataFrame) -> DataFrame:
        return PropertySchema.flatten_property_data(df)

    def process_data(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Validates and flattens the incoming bronze DataFrame.
        Returns a tuple of (valid_df, invalid_df).
        """
        flattened_df = self.flatten_property_data(df)
        valid_df, invalid_df = self.validator.validate_and_quarantine(flattened_df)
        return valid_df, invalid_df

    def transform_transaction_facts(self, df: DataFrame) -> DataFrame:
        # Transform flattened data into transaction fact table
        fact_df = (
            df.select(
                "property_id",  
                "transaction_id",
                "sale_price_eur",
                "transaction_date",
                F.col("price_per_sqm_eur").alias("transaction_price_per_sqm_eur"),
                "interest_rate_percent",
                "demand_index"
            )
        )
        return fact_df

    def transform_property_dimensions(self, df: DataFrame) -> DataFrame:
        # Transform flattened data into property dimension table
        dim_df = (
            df.select(
                "property_id",
                "street",
                "postal_code",
                "borough",
                "latitude",
                "longitude",
                "property_type",
                "size_sqm",
                "lot_size_sqm",
                "bedrooms",
                "bathrooms",
                "total_rooms",
                "construction_year",
                "renovation_year",
                "condition",
                "balcony",
                "elevator",
                "heating_type",
                "parking",
                "energy_rating",
                "energy_consumption_kwh_m2",
                "public_transport_m",
                "schools_m",
                "parks_m",
                "median_income_eur",
                "population_density_per_sqm",
                "crime_rate_per_1000",
                "avg_price_per_sqm_2003_eur",
                "avg_price_per_sqm_2025_eur",
                "cap_rate_percent",
                "rental_yield_percent"
            )
        )
        return dim_df