#import all tables
from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import ConfigParser
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
config=ConfigParser()
config.read(r"C:\Users\chinm\OneDrive\Pictures\details_file.txt")
host=config.get('cred','host')
usr=config.get('cred','usr')
pwd=config.get('cred','pwd')
date=config.get('input','data')
qry=config.get('proc','qry1')

df=spark.read.format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('query',qry)\
    .option('driver',"com.mysql.jdbc.Driver").load()
tabs=[x[0] for x in df.collect()]
for i in tabs:
    df1 = spark.read.format('jdbc').option('url', host).option('user', usr).option('password', pwd).option('dbtable', i) \
        .option('driver', "com.mysql.jdbc.Driver").load()
    df1.show()