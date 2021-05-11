import pyspark
import os

print('test chain')

exampleDir = os.path.join(os.environ["SPARK_HOME"], "jars")
exampleJars = [os.path.join(exampleDir, x) for x in os.listdir(exampleDir)]

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars", ",".join(exampleJars)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.databricks.delta.stateReconstructionValidation.enabled", "false") \
    .getOrCreate()

spark.sparkContext.addPyFile("/spark/jars/delta-core_2.11-0.6.0.jar")
from delta.tables import *

data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table")