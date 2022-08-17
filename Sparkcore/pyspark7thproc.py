#extracting Data from query approach with multiple conditions
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv"
rdd=sc.textFile(data)
res=rdd.map(lambda x:x.split(',')).filter(lambda x:"age" not in x).toDF(["name","age","city"])
res.createOrReplaceTempView('Table')  #####Giving name to dataframe..
result=spark.sql("select * from Table where age>=30 and city='hyd'")
result.show()