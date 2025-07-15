import json
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pipeline.gold.gold_properties import GoldProcessor

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("gold-test").getOrCreate()

@pytest.fixture(scope="module")
def processor(spark):
    return GoldProcessor(spark)

def load_json_to_df(spark, path):
    with open(path, "r") as f:
        data = json.load(f)
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(row) for row in data]))

def assert_df_equal(result_df, expected_df):
    # Normalize numeric types (e.g. long vs double)
    for col in result_df.columns:
        if col in expected_df.columns:
            result_type = dict(result_df.dtypes)[col]
            expected_type = dict(expected_df.dtypes)[col]
            if result_type != expected_type:
                # Force both to double for comparison
                result_df = result_df.withColumn(col, F.col(col).cast(DoubleType()))
                expected_df = expected_df.withColumn(col, F.col(col).cast(DoubleType()))

    # Align column order after normalization
    result_df = result_df.select(sorted(result_df.columns))
    expected_df = expected_df.select(sorted(expected_df.columns))

    # Debugging aid: print schemas if mismatch
    if result_df.schema != expected_df.schema:
        print("\nEXPECTED SCHEMA:")
        expected_df.printSchema()
        print("\nRESULT SCHEMA:")
        result_df.printSchema()

    assert result_df.schema == expected_df.schema, (
        "Schema mismatch\n\n"
        f"Expected schema:\n{expected_df.schema}\n\n"
        f"Got schema:\n{result_df.schema}"
    )

    result_data = [row.asDict(recursive=True) for row in result_df.collect()]
    expected_data = [row.asDict(recursive=True) for row in expected_df.collect()]

    assert result_data == expected_data, (
        "\nData mismatch between result and expected:"
        f"\n\nExpected:\n{json.dumps(expected_data, indent=2)}"
        f"\n\nGot:\n{json.dumps(result_data, indent=2)}"
    )

def test_aggregate_property_valuations(spark, processor):
    fact_path = "tests/unit/gold/data/gold_properties/input_aggregate_property_valuations_fact.json"
    dim_path = "tests/unit/gold/data/gold_properties/input_aggregate_property_valuations_dim.json"
    expected_path = "tests/unit/gold/data/gold_properties/expected_aggregate_property_valuations.json"

    fact_df = load_json_to_df(spark, fact_path)
    dim_df = load_json_to_df(spark, dim_path)
    expected_df = load_json_to_df(spark, expected_path)

    result_df = processor.aggregate_property_valuations(fact_df, dim_df)
    assert_df_equal(result_df, expected_df)

def test_build_neighborhood_trends(spark, processor):
    input_path = "tests/unit/gold/data/gold_properties/input_build_neighborhood_trends_dim.json"
    expected_path = "tests/unit/gold/data/gold_properties/expected_build_neighborhood_trends.json"

    input_df = load_json_to_df(spark, input_path)
    expected_df = load_json_to_df(spark, expected_path)

    result_df = processor.build_neighborhood_trends(input_df)
    assert_df_equal(result_df, expected_df)