from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
    ArrayType
)

bronze_property_schema = StructType([
    StructField("property_id", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("borough", StringType(), True)
    ]), True),
    StructField("coordinates", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),
    StructField("property_type", StringType(), True),
    StructField("size_sqm", DoubleType(), True),
    StructField("lot_size_sqm", DoubleType(), True),
    StructField("rooms", StructType([
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("total_rooms", IntegerType(), True)
    ]), True),
    StructField("construction_year", IntegerType(), True),
    StructField("renovation_year", IntegerType(), True),
    StructField("condition", StringType(), True),
    StructField("features", StructType([
        StructField("balcony", BooleanType(), True),
        StructField("elevator", BooleanType(), True),
        StructField("heating_type", StringType(), True),
        StructField("parking", BooleanType(), True)
    ]), True),
    StructField("energy_rating", StringType(), True),
    StructField("energy_consumption_kwh_m2", DoubleType(), True),
    StructField("transaction_history", ArrayType(StructType([
        StructField("transaction_id", StringType(), True),
        StructField("sale_price_eur", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("price_per_sqm_eur", DoubleType(), True),
        StructField("market_conditions", StructType([
            StructField("interest_rate_percent", DoubleType(), True),
            StructField("demand_index", IntegerType(), True)
        ]), True)
    ])), True),
    StructField("proximity", StructType([
        StructField("public_transport_m", IntegerType(), True),
        StructField("schools_m", IntegerType(), True),
        StructField("parks_m", IntegerType(), True)
    ]), True),
    StructField("neighborhood_metrics", StructType([
        StructField("median_income_eur", IntegerType(), True),
        StructField("population_density_per_sqm", IntegerType(), True),
        StructField("crime_rate_per_1000", DoubleType(), True)
    ]), True),
    StructField("market_trends", StructType([
        StructField("avg_price_per_sqm_2003_eur", DoubleType(), True),
        StructField("avg_price_per_sqm_2025_eur", DoubleType(), True),
        StructField("cap_rate_percent", DoubleType(), True),
        StructField("rental_yield_percent", DoubleType(), True)
    ]), True)
])