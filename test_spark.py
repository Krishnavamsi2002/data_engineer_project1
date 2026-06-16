from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
import shutil

# Create a stable local temp dir for Spark to use (avoids Windows Temp race/deletion issues)
_SPARK_LOCAL_TMP = os.path.join(os.getcwd(), "spark_local_tmp")
os.makedirs(_SPARK_LOCAL_TMP, exist_ok=True)

try:
    spark = SparkSession.builder \
        .appName("Test_spark") \
        .master("local[*]") \
        .config("spark.local.dir", _SPARK_LOCAL_TMP) \
        .getOrCreate()

    data = [("AA", 10), ("BB", 20), ("CC", 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()

finally:
    if 'spark' in locals() and spark:
        spark.stop()

    # Try to remove the local temp directory we created. Ignore if already deleted.
    try:
        shutil.rmtree(_SPARK_LOCAL_TMP)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Warning: could not remove {_SPARK_LOCAL_TMP}: {e}")