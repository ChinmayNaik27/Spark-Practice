#getting concating output
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
ndf=df.withColumn("Newcol",concat_ws(' ',col('city'),col('State')))
ndf.show(30,truncate=True)
#upadating existing Column
ndf1=df.withColumn('Postal Code',concat_ws(' -- ',col('Postal Code'),col('region')))
ndf1.show()