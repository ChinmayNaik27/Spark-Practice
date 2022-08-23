#connecting to db and fetchinga data
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb"
uname='myuser'
pwd='password'
df=spark.read.format('jdbc').option('url',host).option('user',uname)\
    .option('password',pwd).option('driver','com.mysql.jdbc.Driver').option('dbtable','asltab').load()
df.show()
df1=spark.read.format('jdbc').option('url',host).option('user',uname).option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').option('dbtable','asltabnew').load()
df1.show()