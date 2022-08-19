from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\Downloads\\bank-full.csv"
#specifying datatypes
res=spark.read.format('csv').option('header','true').option('sep',';').option('inferschema','true').load(data)

res.printSchema()
res.show()