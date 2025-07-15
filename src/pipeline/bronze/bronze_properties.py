from pyspark.sql import SparkSession, DataFrame
from bronze_schema import bronze_property_schema

class BronzeProcessor:
    def __init__(self, spark: SparkSession, gcs_path: str, target_schema: str, dbutils):
        self.spark = spark
        self.gcs_path = gcs_path
        self.target_schema = target_schema
        self.dbutils = dbutils

    def ingest_raw_property_data(self) -> DataFrame:
        """
        Ingest nested JSON files from GCS using Databricks Auto Loader.
        GCS credentials are configured via the cluster's service account.
        """

        df = (
            self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("multiline", "true")
                .option("cloudFiles.includeExistingFiles", "true")
                .option("cloudFiles.schemaLocation", f"{self.gcs_path}/_schema")
                .option("cloudFiles.validateOptions", "true")
                .option("cloudFiles.useNotifications", "false")
                .option("cloudFiles.useIncrementalListing", "true")
                .schema(bronze_property_schema)
                .load(self.gcs_path)
        )

        return df
