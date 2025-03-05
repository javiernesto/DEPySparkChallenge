import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType


class EngagementProcessor:
    """
    Webpage engagement data processor class
    """

    def __init__(self):
        self._data = None

    @property
    def data(self):
        """
        Property to get the data variable of the class
        :return: PySpark Dataframe
        """
        return self._data

    def read_from_csv(self, spark, file_path):
        """
        Reads a csv file and load it to the _data PySpark Dataframe from the class
        :param spark: PySpark Session
        :param file_path: Path to the file
        """
        schema = StructType() \
            .add("user_id", IntegerType(), True) \
            .add("timestamp", TimestampType(), True) \
            .add("page", StringType(), True) \
            .add("duration_seconds", IntegerType(), True)

        try:
            self._data = spark.read \
                .format("csv") \
                .option("header", True) \
                .schema(schema) \
                .load(file_path)
        except Exception as ex:
            print(str(ex))
            self._data = None

    def get_avg_page_duration(self):
        """
        Calculates the average duration users spent on the webpages
        :return: PySpark Dataframe
        """
        df_result = None
        try:
            df_result = self._data.groupBy("page") \
                .agg(psf.avg("duration_seconds").alias("avg_duration_sec")) \
                .orderBy(psf.desc("avg_duration_sec"))
        except Exception as ex:
            print(str(ex))

        return df_result

    def get_most_engaging_page(self):
        """
        Calculates the most engaging page
        :return: Tuple
        """
        result = (None, None)
        try:
            df_max = self.get_avg_page_duration()
            result = (df_max.collect()[0][0], df_max.collect()[0][1])
        except Exception as ex:
            print(str(ex))

        return result
