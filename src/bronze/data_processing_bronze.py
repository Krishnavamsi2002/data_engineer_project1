import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pathlib import Path

def load_data():
    spark = SparkSession.builder \
        .appName("RetailDataProcessing") \
        .master("local[*]") \
        .getOrCreate()
    data_frame = spark.read.csv(
       "D:\\projects\\data-engineering-project\\data\\raw\\retail_sales_dataset.csv",
        header=True,
        inferSchema=True

    )
    return data_frame

def main():
    df = load_data()
    df.show(5)

if __name__ == "__main__":
    main()