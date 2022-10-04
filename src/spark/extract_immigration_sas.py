"""
Spark job designed to extract SAS format i94 immigration and global temperature  data and write it to Amazon S3 in Parquet format
"""

import configparser
import os
import datetime
import calendar
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']

I94_INPUT_PATH = config['AWS']['I94_INPUT_PATH']
TEMP_INPUT_PATH = config['AWS']['TEMP_INPUT_PATH']
OUTPUT_PATH = config['AWS']['OUTPUT_PATH']


def create_spark_session():
    """
    Creates Spark session
    """
    spark = SparkSession \
        .builder \
        .appName("Extract and clean i94 Data") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def create_local_spark_session():
    """
    Creates local Spark session
    :return: Spark session configured for local mode
    """
    number_cores = 8
    memory_gb = 8

    spark = SparkSession \
        .builder \
        .appName("Extract and clean i94 Data") \
        .master('local[{}]'.format(number_cores)) \
        .config('spark.driver.memory', '{}g'.format(memory_gb)) \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def convert_sas_date(days):
    """
    Converts SAS date stored as days since 1/1/1960 to datetime
    :param days: Days since 1/1/1960
    :return: datetime
    """
    if days is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=days)


def get_sas_day(days):
    """
    Converts SAS date stored as days since 1/1/1960 to day of month
    :param days: Days since 1/1/1960
    :return: Day of month value as integer
    """
    if days is None:
        return None
    return (datetime.date(1960, 1, 1) + datetime.timedelta(days=days)).day


def convert_i94mode(mode):
    """
    Converts i94 travel mode code to a description
    :param mode: int i94 mode as integer
    :return: i94 mode description
    """
    if mode == 1:
        return "Air"
    elif mode == 2:
        return "Sea"
    elif mode == 3:
        return "Land"
    else:
        return "Not Reported"


def convert_visa(visa):
    """
    Converts visa numeric code to description
    :param visa: str
    :return: Visa description: str
    """
    if visa is None:
        return "Not Reported"
    elif visa == 1:
        return "Business"
    elif visa == 2:
        return "Pleasure"
    elif visa == 3:
        return "Student"
    else:
        return "Not Reported"


def clean_i94_data(spark, input_path, output_path):
    """
    Loads SAS i94 immigration data into data frame.
    Data is cleaned and projected, and then written to Parquet.
    Partitioned by year, month, and day.
    :param spark: Spark session
    :param input_path: Input path to SAS data
    :param output_path: Output path for Parquet files
    :return: None
    """

    convert_i94mode_udf = F.udf(convert_i94mode, StringType())
    convert_sas_date_udf = F.udf(convert_sas_date, DateType())
    convert_visa_udf = F.udf(convert_visa, StringType())
    get_sas_day_udf = F.udf(get_sas_day, IntegerType())

    months = list(v.lower() for v, k in zip(calendar.month_abbr[1:], range(1, 13)))

    for month in months:
        input_path = f"{input_path}/i94_{month}16_sub.sas7bdat"
        print(f"Cleaning {input_path}...")

        # Read SAS data for month
        df = spark.read.format("com.github.saurfang.sas.spark").load(input_path)

        # Set appropriate names and data types for columns
        df = df.withColumn('arrival_date', convert_sas_date_udf(df['arrdate'])) \
            .withColumn('departure_date', convert_sas_date_udf(df['depdate'])) \
            .withColumn('year', df['i94yr'].cast(IntegerType())) \
            .withColumn('month', df['i94mon'].cast(IntegerType())) \
            .withColumn('arrival_day', get_sas_day_udf(df['arrdate'])) \
            .withColumn('age', df['i94bir'].cast(IntegerType())) \
            .withColumn('country_code', df['i94cit'].cast(IntegerType()).cast(StringType())) \
            .withColumn('port_code', df['i94port'].cast(StringType())) \
            .withColumn('birth_year', df['biryear'].cast(IntegerType())) \
            .withColumn('mode', convert_i94mode_udf(df['i94mode'])) \
            .withColumn('visa_category', convert_visa_udf(df['i94visa']))

        # Project final data set
        immigration_df = df.select(
            ['year', 'month', 'arrival_day', 'age', 'country_code', 'port_code', 'mode', 'visa_category', 'visatype',
             'gender',
             'birth_year', 'arrdate', 'arrival_date', 'depdate', 'departure_date'])

        # Write data in Apache Parquet format partitioned by year, month, and day
        print(f"Writing {input_path} to output...")
        immigration_df.write.mode("append").partitionBy("year", "month", "arrival_day") \
            .parquet(f"{output_path}/immigration_data")
        print(f"Completed {input_path}.")


def extract_city_temp_data(spark, input_path, output_path):
    """
    Load GlobalLandTemperaturesByCity
    Extract latest temperature for U.S. cities
    Write to Parquet format
    :param spark: Spark Session
    :param input_path: 
    :param output_path:
    :return:
    """
    temp_df = spark.read.option("header", True).option("inferSchema",True).csv(f"{input_path}/GlobalLandTemperaturesByCity.csv")
    temp_df = temp_df.filter(temp_df.AverageTemperature.isNotNull())
    temp_df = temp_df.filter(temp_df.Country == "United States") \
        .withColumn("rank", F.dense_rank().over(Window.partitionBy("City").orderBy(F.desc("dt"))))
    temp_df = temp_df.filter(temp_df["rank"] == 1).orderBy("City")
    temp_df.write.mode("overwrite").parquet(f"{output_path}/us_city_temperature_data")

def extract_country_temp_data(spark, input_path, output_path):
    """
    Load GlobalLandTemperaturesByCountry
    Extract latest temperature for each country
    Write to Parquet format
    :param spark: Spark Session
    :param input_path:
    :param output_path:
    :return:
    """
    temp_df = spark.read.option("header", True).option("inferSchema",True).csv(f"{input_path}/GlobalLandTemperaturesByCountry.csv")
    temp_df = temp_df.filter(temp_df.AverageTemperature.isNotNull())
    temp_df = temp_df.withColumn("rank", F.dense_rank().over(Window.partitionBy("Country").orderBy(F.desc("dt"))))
    temp_df = temp_df.filter(temp_df["rank"] == 1).orderBy("Country")
    temp_df.write.mode("overwrite").parquet(f"{output_path}/country_temperature_data")



spark = create_spark_session()
clean_i94_data(spark, I94_INPUT_PATH, OUTPUT_PATH)
extract_country_temp_data(spark, TEMP_INPUT_PATH, OUTPUT_PATH)
extract_city_temp_data(spark, TEMP_INPUT_PATH, OUTPUT_PATH)