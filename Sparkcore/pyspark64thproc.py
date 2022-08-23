#reading data from file
from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import ConfigParser
conf=ConfigParser()
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
conf.read(r"C:\Users\chinm\OneDrive\Pictures\details_file.txt")
data=conf.get("input","data")
host=conf.get('cred','host')
usr=conf.get('cred','usr')
pwd=conf.get('cred','pwd')
qry=conf.get('proc','qry')
df=spark.read.format("jdbc").option('url',host).option('user',usr).option('password',pwd).option('query',qry)\
    .option('driver',"com.mysql.jdbc.Driver").load()
df.show()

