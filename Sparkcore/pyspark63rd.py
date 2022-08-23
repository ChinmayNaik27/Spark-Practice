from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

tabs=['asltab','DEPT','banktab']
host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
usr='myuser'
pwd='password'
for i in tabs:
    df=spark.read.format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('driver',"com.mysql.jdbc.Driver").option('dbtable',i).load()
    df.show()