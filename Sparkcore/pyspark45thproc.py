#extrating year form the given column
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").config('spark.sql.session.timezone','EST').getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")

ndf=df.withColumn('Order Date',to_date(col('Order Date'),'d-M-yyyy')).withColumn("year",year(col('Order Date')))
ndf.show()