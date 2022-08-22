##createing current date of outer server according to client
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test1").config("spark.sql.session.timezone","EST").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn('Current Date',current_date()).withColumn("Timestamp",current_timestamp())
ndf.show(truncate=False)