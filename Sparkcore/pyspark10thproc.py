#extracting data using programing friendly enviornment i.e using where and multiple conditions
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv"
rdd=sc.textFile(data)
res=rdd.map(lambda x:x.split(',')).filter(lambda x:"age" not in x).toDF(["name","age","city"])
result=res.where((col('age')>=30) & (col('city')=='hyd'))
result.show()


