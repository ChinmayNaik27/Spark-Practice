#storing a csv file to the table
import re

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"

usr='myuser'
pwd='password'
df=spark.read.format('csv').option('header','true').option('inferShema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
cols=[re.sub(r'[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)
ndf.show()
# df.show()
ndf.write.format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('driver',"com.mysql.jdbc.Driver").option('dbtable','orders').save()
ndf.printSchema()

#loads dataframe to mysql
df.write.format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('driver',"com.mysql.jdbc.Driver").option('dbtable','orders1').save()
df.printSchema()