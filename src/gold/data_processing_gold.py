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
    
    path = Path("D:\\projects\\data-engineering-project\\data\\bronze\\processed_data.csv")
    data_frame = spark.read.csv(str(path),
       header=True,
       inferSchema=True
    )
    data_frame.show(5)
    return data_frame

def process_data_per_month(df):
    df=df.groupBy("Month_name").agg(F.sum("Total_Amount").alias("Total_Amount_per_Month"))  

    return df

def main():
    df = load_data()
    processed_df = process_data_per_month(df)
    processed_df.show(5)

if __name__ == "__main__":
    main()