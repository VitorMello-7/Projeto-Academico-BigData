from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder.appName("BigDataTask").getOrCreate()

sample_data = [{"name": "Alice", "age": 30}, 
               {"name": "Bob", "age": 25},
               {"name": "Cathy", "age": 27},
               {"name": "David", "age": 35}]

df = spark.createDataFrame(sample_data)

df.show()