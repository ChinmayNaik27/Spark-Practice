#using having conditions
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)
df.show()
ndf=df.groupBy(col('Country')).agg(avg('Sales'),min('Sales'),max('Sales'),sum('Sales')).where(sum('Sales')>avg('Sales'))
ndf.show()