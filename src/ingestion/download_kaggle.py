#new future branch for kaggle download and ingestion and puhing to develop branch
#giting code from kaggle branch and moving to develop branch
import os
import zipfile
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pathlib import Path

print("Starting Kaggle dataset download and ingestion...")

# Load environment variables
load_dotenv(Path(".env"))   
key = os.getenv("KAGGLE_KEY")
username = os.getenv("KAGGLE_USERNAME")

# Kaggle credentials
os.environ["KAGGLE_USERNAME"] = username
os.environ["KAGGLE_KEY"] = key

# Project structure
raw_folder = Path("data/raw")
raw_folder.mkdir(parents=True, exist_ok=True)

def download_kaggle():
    # Kaggle dataset
    dataset_slug = "mohammadtalib786/retail-sales-dataset"

    # Download dataset
    api = KaggleApi()
    api.authenticate()

    print("Downloading dataset...")
    api.dataset_download_files(
        dataset_slug,
        path=str(raw_folder),
        unzip=False
    )

    # Extract zip file
    zip_file = next(raw_folder.glob("*.zip"))

    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(raw_folder)

    print("Dataset extracted successfully.")
def spark_ingest_data():

    spark = SparkSession.builder \
        .appName("RetailDataIngestion") \
        .master("local[*]") \
        .getOrCreate()

    # Read CSV
    csv_file = next(raw_folder.glob("*.csv"))

    df = spark.read.csv(
        str(csv_file),
        header=True,
        inferSchema=True
    )

    print("Schema:")
    df.printSchema()

    print("Sample Data:")
    df.show(5, truncate=False)

    spark.stop()

def main():
    download_kaggle()
    spark_ingest_data()

if __name__ == "__main__":
    main()  
