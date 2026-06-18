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
    data_frame.show(5)
    return data_frame

def process_data_per_month(df):
    df=df.withColumn("Month_name",F.date_format("Date", "MMMM")) 

    df=df.groupBy("Month_name").agg(F.sum("Total Amount").alias("Total_Sales_per_Month"))
    df.show(5)
    return df

def save_data(df):
    output_path = Path("data/bronze/processed_data")
    output_path.mkdir(parents=True, exist_ok=True)
    df.write.csv(str(output_path), header=True, mode="overwrite")

def main():
    df = load_data()
    processed_df = process_data_per_month(df)
    #save_data(processed_df)

if __name__ == "__main__":
    main()