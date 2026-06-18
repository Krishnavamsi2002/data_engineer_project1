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
    tup=(("Transaction ID", "Transaction_ID"),("Product Category", "Product_Category"),("Customer ID", "Customer_ID"),\
         ("Price per Unit", "Price_per_Unit"),("Total Amount", "Total_Amount"))
    for old_col, new_col in tup:
        df=df.withColumnRenamed(old_col, new_col)

    try:
        df=df.withColumn("Date", F.to_date("Date", "yyyy-MM-dd"))
        df=df.withColumn("Month_name",F.date_format("Date", "MMMM"))
    except Exception as e:
        print(f"Error processing date: {e}")
    
    print(f"Processed data row count: {df.count()}")
    df.show(5)
   
    return df

def save_data(df):
    output_path = "D:\\projects\\data-engineering-project\\data\\bronze\\processed_data"
    print(f"Saving processed data to form save_data: {output_path}")
    print(f"DataFrame row count: {df.count()}")
    row_count = df.count()
    df.show(5)
    
    if row_count > 0:
        try:
            print("Data saving!")
            df.coalesce(1).write.csv(str(output_path), header=True, mode="overwrite")
            print("Data saved successfully!")
            print(f"Files saved to: {output_path}")
        except Exception as e:
            print(f"ERROR during write: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("ERROR: DataFrame is empty! No data to save.")

def main():
    df = load_data()
    processed_df = process_data_per_month(df)
    save_data(processed_df)

if __name__ == "__main__":
    main()