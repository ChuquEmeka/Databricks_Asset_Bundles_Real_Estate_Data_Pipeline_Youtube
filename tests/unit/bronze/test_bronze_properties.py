import json
import pytest
from pyspark.sql import SparkSession
from pipeline.bronze.bronze_properties import BronzeProcessor
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../src")))

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("BronzeProcessorTest").getOrCreate()

def load_json_as_df(spark, path):
    with open(path, "r") as f:
        data = json.load(f)
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(row) for row in data]))

def test_ingest_raw_property_data_schema_and_content(spark):
    input_path = "tests/unit/bronze/data/bronze_properties/input_ingest_raw_property_data.json"
    expected_path = "tests/unit/bronze/data/bronze_properties/expected_ingest_raw_property_data.json"

    input_df = load_json_as_df(spark, input_path)
    expected_df = load_json_as_df(spark, expected_path)

    class MockDbutils:
        pass

    processor = BronzeProcessor(spark, gcs_path="unused", target_schema="unused", dbutils=MockDbutils())
    processor.ingest_raw_property_data = lambda: input_df

    result_df = processor.ingest_raw_property_data()

    assert result_df.schema == expected_df.schema, "Schema mismatch between ingested and expected data"

    result_row = result_df.collect()[0].asDict(recursive=True)
    expected_row = expected_df.collect()[0].asDict(recursive=True)

    assert result_row == expected_row, (
        "\nData mismatch between ingested and expected row:"
        f"\n\nExpected:\n{json.dumps(expected_row, indent=2)}"
        f"\n\nGot:\n{json.dumps(result_row, indent=2)}"
    )