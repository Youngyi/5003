from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local",'app')
spark = SparkSession.builder.appName('name').config('spark.sql.shuffle.partitions',10).getOrCreate()


# RDD

# 1.transformation
# filter
shortFruits = fruits.filter(lambda fruit: len(fruit) <= 5)
