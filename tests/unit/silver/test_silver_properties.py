
import json
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pipeline.silver.silver_properties import SilverProcessor

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("SilverProcessorTest").getOrCreate()

def load_json_to_df(spark, path):
    with open(path, "r") as f:
        data = json.load(f)
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(row) for row in data]))

@pytest.fixture(scope="module")
def processor(spark):
    return SilverProcessor(spark, "br_properties")

def assert_df_equal(result_df, expected_df):
    # Align column order
    result_df = result_df.select(sorted(result_df.columns))
    expected_df = expected_df.select(sorted(expected_df.columns))

    # Align types for known fields
    if "transaction_date" in result_df.columns and "transaction_date" in expected_df.columns:
        result_type = dict(result_df.dtypes)["transaction_date"]
        expected_type = dict(expected_df.dtypes)["transaction_date"]

        if result_type != expected_type:
            if result_type == "string" and expected_type == "date":
                result_df = result_df.withColumn("transaction_date", F.to_date("transaction_date"))
            elif result_type == "date" and expected_type == "string":
                expected_df = expected_df.withColumn("transaction_date", F.to_date("transaction_date"))

    assert result_df.schema == expected_df.schema, "Schema mismatch"

    result_data = [row.asDict(recursive=True) for row in result_df.collect()]
    expected_data = [row.asDict(recursive=True) for row in expected_df.collect()]

    assert result_data == expected_data, (
        "\nData mismatch between result and expected:"
        f"\n\nExpected:\n{json.dumps(expected_data, indent=2)}"
        f"\n\nGot:\n{json.dumps(result_data, indent=2)}"
    )

def test_flatten_property_data(spark, processor):
    input_path = "tests/unit/silver/data/silver_properties/input_flatten_property_data.json"
    expected_path = "tests/unit/silver/data/silver_properties/expected_flatten_property_data.json"
    input_df = load_json_to_df(spark, input_path)
    expected_df = load_json_to_df(spark, expected_path)
    result_df = processor.flatten_property_data(input_df)
    assert_df_equal(result_df, expected_df)

def test_process_data(spark, processor):
    input_path = "tests/unit/silver/data/silver_properties/input_flatten_property_data.json"
    input_df = load_json_to_df(spark, input_path)
    valid_df, invalid_df = processor.process_data(input_df)

    assert valid_df.count() == 1, "Expected 1 valid row"
    assert invalid_df.count() == 0, "Expected 0 invalid rows"
    assert "is_quarantined" in invalid_df.columns, "'is_quarantined' column missing in invalid_df"

def test_transform_transaction_facts(spark, processor):
    input_path = "tests/unit/silver/data/silver_properties/input_transform_transaction_facts.json"
    expected_path = "tests/unit/silver/data/silver_properties/expected_transform_transaction_facts.json"
    input_df = load_json_to_df(spark, input_path)
    expected_df = load_json_to_df(spark, expected_path)
    result_df = processor.transform_transaction_facts(input_df)
    assert_df_equal(result_df, expected_df)

def test_transform_property_dimensions(spark, processor):
    input_path = "tests/unit/silver/data/silver_properties/input_transform_property_dimensions.json"
    expected_path = "tests/unit/silver/data/silver_properties/expected_transform_property_dimensions.json"
    input_df = load_json_to_df(spark, input_path)
    expected_df = load_json_to_df(spark, expected_path)
    result_df = processor.transform_property_dimensions(input_df)
    assert_df_equal(result_df, expected_df)