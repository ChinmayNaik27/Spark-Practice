from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:/Users/chinm/Downloads/bank-full - Copy.csv"
rdd=sc.textFile(data)
skip=rdd.first()
odata=rdd.map(lambda x:x.replace("\"","")).filter(lambda x:x!=skip)
res=spark.read.csv(odata,header=True,inferSchema=True,sep=";")
# res=spark.read.format('csv').option('header','true').option('inferSchema','true').option('sep',';').load(odata)
res.show()
