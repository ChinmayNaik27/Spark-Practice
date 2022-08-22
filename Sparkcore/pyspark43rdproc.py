#using multiple aggregate functions
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
df.show()
ndf =df.groupBy(col('state')).agg(avg(col('Profit')).alias('avg profit'),max(col('profit')).alias('max profit'),min(col('profit').alias('min profit')))
ndf.show()
#methord 2nd
df.describe().show()    #gives you aggregate for all columns
df.describe('Profit').show()   #gives you aggregate for profit column
df.describe('Profit','Country').show() #gives you aggregate for profit and country column