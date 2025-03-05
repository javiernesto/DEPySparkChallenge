from datetime import datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType, DoubleType

from src.engagement_processor import EngagementProcessor


@pytest.fixture(scope="module")
def file_path():
    """
    Define a fixture to return the test file path
    :return: test file path string
    """
    return "../data/test_data.csv"


@pytest.fixture(scope="module")
def spark_session():
    """
    Define a fixture to create a Spark session at the module level.
    :return: Spark session
    """
    spark = SparkSession.builder.appName("PySparkChallengeTest").getOrCreate()
    yield spark
    spark.stop()


def test_read_from_csv_count(spark_session, file_path):
    """
    Test the read_from_csv method from the EngagementProcessor class verifying the record count
    :param spark_session: PySpark Session
    :param file_path: Path to the file
    """
    e_processor = EngagementProcessor()
    e_processor.read_from_csv(spark_session, file_path)
    df_data = e_processor.data
    assert df_data.count() == 6


def test_read_from_csv_schema(spark_session, file_path):
    """
    Test the read_from_csv method from the EngagementProcessor class verifying the returned schema and data
    :param spark_session: PySpark Session
    :param file_path: Path to the file
    """
    e_processor = EngagementProcessor()
    e_processor.read_from_csv(spark_session, file_path)
    df_data = e_processor.data

    data = [
        (1, datetime.strptime("2022-01-01 12:00:00", "%Y-%m-%d %H:%M:%S"), "home", 10),
        (2, datetime.strptime("2022-01-01 12:05:00", "%Y-%m-%d %H:%M:%S"), "dashboard", 30),
        (1, datetime.strptime("2022-01-02 12:00:00", "%Y-%m-%d %H:%M:%S"), "profile", 5),
        (2, datetime.strptime("2022-01-02 12:05:00", "%Y-%m-%d %H:%M:%S"), "profile", 45),
        (1, datetime.strptime("2022-01-03 12:00:00", "%Y-%m-%d %H:%M:%S"), "home", 20),
        (2, datetime.strptime("2022-01-03 12:05:00", "%Y-%m-%d %H:%M:%S"), "dashboard", 25)
        ]

    columns = StructType() \
        .add("user_id", IntegerType(), True) \
        .add("timestamp", TimestampType(), True) \
        .add("page", StringType(), True) \
        .add("duration_seconds", IntegerType(), True)

    df_expected = spark_session.createDataFrame(data=data, schema=columns)
    assert_df_equality(df_data, df_expected)


def test_get_average_page_duration(spark_session, file_path):
    """
    Test the get_avg_page_duration method from the EngagementProcessor class verifying the returned schema and data
    :param spark_session: PySpark Session
    :param file_path: Path to the file
    """
    e_processor = EngagementProcessor()
    e_processor.read_from_csv(spark_session, file_path)

    data_expected = [
        ("dashboard", 27.5),
        ("profile", 25.0),
        ("home", 15.0)
        ]

    columns_expected = StructType() \
        .add("page", StringType(), True) \
        .add("avg_duration_sec", DoubleType(), True)

    df_expected = spark_session.createDataFrame(data=data_expected, schema=columns_expected)

    df_transformed = e_processor.get_avg_page_duration()
    assert_df_equality(df_transformed, df_expected)


def test_get_most_engaging_page(spark_session, file_path):
    """
    Test the get_most_engaging_page method from the EngagementProcessor class verifying the returned string
    :param spark_session: PySpark Session
    :param file_path: Path to the file
    """
    e_processor = EngagementProcessor()
    e_processor.read_from_csv(spark_session, file_path)
    expected = ("dashboard", 27.5)
    actual = e_processor.get_most_engaging_page()
    assert expected == actual
