#data processing
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\Users\\chinm\\Downloads\\bank-full.csv"

df=spark.read.format('csv').option('header','true').option('sep',';').option('inferSchema','true').load(data)
#data processing using where clause
res=df.where(col('age')>90)
res.show()
res.printSchema()
res.show(25)   #shows 1st 25 records
#shows first 25 records with character
res.show(20,truncate=True)