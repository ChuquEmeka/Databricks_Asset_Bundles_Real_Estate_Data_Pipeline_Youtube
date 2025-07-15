
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

class PropertySchema:
    @staticmethod
    def flatten_property_data(df: DataFrame) -> DataFrame:
        # Flatten top-level and nested fields
        flattened_df = df.select(
            "property_id",
            F.col("address.street").alias("street"),
            F.col("address.postal_code").alias("postal_code"),
            F.col("address.borough").alias("borough"),
            F.col("coordinates.latitude").alias("latitude"),
            F.col("coordinates.longitude").alias("longitude"),
            "property_type",
            "size_sqm",
            "lot_size_sqm",
            F.col("rooms.bedrooms").alias("bedrooms"),
            F.col("rooms.bathrooms").alias("bathrooms"),
            F.col("rooms.total_rooms").alias("total_rooms"),
            "construction_year",
            "renovation_year",
            "condition",
            F.col("features.balcony").alias("balcony"),
            F.col("features.elevator").alias("elevator"),
            F.col("features.heating_type").alias("heating_type"),
            F.col("features.parking").alias("parking"),
            "energy_rating",
            "energy_consumption_kwh_m2",
            F.col("proximity.public_transport_m").alias("public_transport_m"),
            F.col("proximity.schools_m").alias("schools_m"),
            F.col("proximity.parks_m").alias("parks_m"),
            F.col("neighborhood_metrics.median_income_eur").alias("median_income_eur"),
            F.col("neighborhood_metrics.population_density_per_sqm").alias("population_density_per_sqm"),
            F.col("neighborhood_metrics.crime_rate_per_1000").alias("crime_rate_per_1000"),
            F.col("market_trends.avg_price_per_sqm_2003_eur").alias("avg_price_per_sqm_2003_eur"),
            F.col("market_trends.avg_price_per_sqm_2025_eur").alias("avg_price_per_sqm_2025_eur"),
            F.col("market_trends.cap_rate_percent").alias("cap_rate_percent"),
            F.col("market_trends.rental_yield_percent").alias("rental_yield_percent"),
            F.explode("transaction_history").alias("transaction")
        )

        # Flatten transaction fields
        return flattened_df.select(
            "*",
            F.col("transaction.transaction_id").alias("transaction_id"),
            F.col("transaction.sale_price_eur").alias("sale_price_eur"),
            F.to_date(F.col("transaction.transaction_date")).alias("transaction_date"),
            F.col("transaction.price_per_sqm_eur").alias("price_per_sqm_eur"),
            F.col("transaction.market_conditions.interest_rate_percent").alias("interest_rate_percent"),
            F.col("transaction.market_conditions.demand_index").alias("demand_index")
        ).drop("transaction")