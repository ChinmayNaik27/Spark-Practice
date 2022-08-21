#converting to dataFrame
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data="D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv"
rdd=sc.textFile(data)
res=rdd.filter(lambda x:"age" not in x).map(lambda x:x.split(',')).toDF(["name",'age','city'])

res.show()