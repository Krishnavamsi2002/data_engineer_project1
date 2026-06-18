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
    #path = Path("D:\\projects\\data-engineering-project\\data\\raw\\retail_sales_dataset.csv")
    data_frame = spark.read.csv(str(path),
       header=True,
       inferSchema=True
    )
    data_frame.show(5)
    return data_frame

def process_data_per_month(df):
    df=df.groupBy("Month_name").agg(F.sum("Total_Amount").alias("Total_Amount_per_Month"))  
    df.show(5)
    return df

def save_data(df):
    output_path = "D:\\projects\\data-engineering-project\\data\\gold\\processed_data_per_month.csv"

    print(f"Saving processed data to form save_data: {output_path}")
    print(f"DataFrame row count: {df.count()}")
    df.show(5)
    if df.count() > 0:
        
        df.coalesce(1).write.csv(str(output_path), header=True, mode="overwrite")
        print("Data saved successfully!")
    else:
        print("ERROR: DataFrame is empty! No data to save.")

def main():
    df = load_data()
    processed_df = process_data_per_month(df)
    save_data(processed_df)
    processed_df.show(5)

if __name__ == "__main__":
    main()