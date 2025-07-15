import json
import pytest
from pyspark.sql import SparkSession, functions as F
from pipeline.silver.silver_properties import SilverProcessor
from pipeline.gold.gold_properties import GoldProcessor

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("full-pipeline-integration").getOrCreate()

def load_json_to_df(spark, path):
    with open(path, "r") as f:
        data = json.load(f)
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(row) for row in data]))

def assert_df_equal(result_df, expected_df):
    for col in result_df.columns:
        if col in expected_df.columns:
            result_type = dict(result_df.dtypes)[col]
            expected_type = dict(expected_df.dtypes)[col]
            if result_type != expected_type:
                result_df = result_df.withColumn(col, F.col(col).cast("double"))
                expected_df = expected_df.withColumn(col, F.col(col).cast("double"))
    result_df = result_df.select(sorted(result_df.columns))
    expected_df = expected_df.select(sorted(expected_df.columns))
    assert result_df.schema == expected_df.schema
    assert [row.asDict() for row in result_df.collect()] == [row.asDict() for row in expected_df.collect()]

def test_full_pipeline(spark):
    # 1. Load input and expected output
    input_path = "tests/integration/full_pipeline/input.json"
    expected_gold_path = "tests/integration/full_pipeline/expected.json"
    expected_trends_path = "tests/integration/full_pipeline/expected_trends.json"

    input_df = load_json_to_df(spark, input_path)
    expected_gold_df = load_json_to_df(spark, expected_gold_path)
    expected_trends_df = load_json_to_df(spark, expected_trends_path)

    # 2. Simulate bronze ingestion
    raw_df = input_df
    raw_df.createOrReplaceTempView("bronze_test_table")

    # 3. Silver transformations
    silver_processor = SilverProcessor(spark, "bronze_test_table")
    flattened_df = silver_processor.flatten_property_data(raw_df)
    facts_df = silver_processor.transform_transaction_facts(flattened_df)
    dims_df = silver_processor.transform_property_dimensions(flattened_df)

    # 4. Gold aggregations
    gold_processor = GoldProcessor(spark)
    gold_agg_df = gold_processor.aggregate_property_valuations(facts_df, dims_df)
    gold_trends_df = gold_processor.build_neighborhood_trends(dims_df)

    # 5. Assertions
    assert_df_equal(gold_agg_df, expected_gold_df)
    assert_df_equal(gold_trends_df, expected_trends_df)