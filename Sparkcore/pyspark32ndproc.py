#rename columnname
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
ndf=df.select(col('Row Id'),col('Ship Date'),col('Country'),col('Customer Name')).withColumnRenamed('Ship Date','shipdate')
ndf.show()