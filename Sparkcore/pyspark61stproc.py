#fetching input from database performing operations and  writing output in database
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname='myuser'
pwd='password'
df=spark.read.format('jdbc').option('url',host).option('user',uname).\
    option('password',pwd).option('driver','com.mysql.jdbc.Driver').option('dbtable','asltabnew').load()
# df.show()
ndf=df.withColumn('date',current_date()).withColumn("timestamp",current_timestamp())
ndf.write.format('jdbc').option("url",host).option('user',uname).option('password',pwd)\
    .option('dbtable','new2').option('driver','com.mysql.jdbc.Driver').save()
ndf.show(truncate=False)

#to ovwewrite existing table
# ndf.write.format('jdbc').mode('overwrite').option('driver','com.mysql.jdbc.Driver').option('url',host).option('user',uname).option('password',pwd).\
    # option('dbtable','new2').save()
# ndf.show()