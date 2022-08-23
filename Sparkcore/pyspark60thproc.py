#filling values instead of nulls
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname='myuser'
pwd='password'
df=spark.read.format('jdbc').option('url',host).option('user',uname).option('password',pwd).option('dbtable','asltab').load()
df.show()
res=df.na.fill('0')
res.show()
res1=df.na.fill('0',['name'])
res1.show()