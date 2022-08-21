#using Query approach
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
rdd=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
rdd.createOrReplaceTempView('tab1')
qry="select *,substring(state,0,4),concat_ws('__',country,city) from tab1"
nrd=spark.sql(qry)
nrd.show()