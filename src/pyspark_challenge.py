from pyspark.sql import SparkSession

from src.engagement_processor import EngagementProcessor


def main():
    spark = SparkSession.builder.appName("PySparkChallenge").getOrCreate()
    file_path = "../data/user_engagement.csv"
    e_processor = EngagementProcessor()
    e_processor.read_from_csv(spark, file_path)
    print("Average Duration Per Page:")
    e_processor.get_avg_page_duration().show()
    most_engaging_page = e_processor.get_most_engaging_page()
    print(
        "Most engaging page: {0} (average duration: {1} seconds)".format(most_engaging_page[0], most_engaging_page[1])
    )


if __name__ == "__main__":
    main()
