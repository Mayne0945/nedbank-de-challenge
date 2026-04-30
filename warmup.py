import os
import pyspark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
builder = (
    SparkSession.builder.master("local[1]").appName("jar-warmup")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.stop()
print("JAR warmup complete.")
