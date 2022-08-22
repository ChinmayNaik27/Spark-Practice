#some other Functions
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)
ndf=df.withColumn('Sl',col('Sales')).withColumn("ceil",ceil(col('Sl'))).withColumn("Floor",floor(col('Sl'))).withColumn("round",round(col('Sl')))
ndf.show()