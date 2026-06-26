from pyspark.sql import SparkSession
from pyspark.sql import functions as F  
from dotenv import load_dotenv
import os

load_dotenv()

def load_data_to_sql(): 
    spark = SparkSession.builder \
        .appName("RetailDataProcessing") \
        .master("local[*]") \
        .config(
            "spark.jars",
            r"D:\projects\data-engineering-project\data\raw\jars\mssql-jdbc-13.4.0.jre11.jar"
        ) \
        .config(
            "spark.driver.extraClassPath",
            r"D:\projects\data-engineering-project\data\raw\jars\mssql-jdbc-13.4.0.jre11.jar"
        ) \
        .getOrCreate()

    # Read the processed data from the bronze layer
    df = spark.read.csv(
        "data/raw/retail_sales_dataset.csv",
        header=True,
        inferSchema=True
    )
    df.show(5)

    # Load data to SQL database
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://{os.getenv('SQL_SERVER')}:1433;"
                f"databaseName={os.getenv('SQL_DATABASE')};trustServerCertificate=true") \
        .option("dbtable", "retail_sales") \
        .option("user", os.getenv('SQL_USERNAME')) \
        .option("password", os.getenv('SQL_PASSWORD')) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("overwrite") \
        .save()

def main():
    load_data_to_sql() 

if __name__ == "__main__":
    main() 

