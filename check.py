import pyspark
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HadoopVersionCheck")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

print("PySpark:", pyspark.__version__)
print(
    "Hadoop:",
    spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
)
print(
    "Java:",
    spark._jvm.java.lang.System.getProperty("java.version")
)

spark.range(5).show()

spark.stop()