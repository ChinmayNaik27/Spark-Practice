#adding row column
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:/bigdata/DATASETS/us-500.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').option('sep',',').load(data)
ndf=df.withColumn("rownum",monotonically_increasing_id())
ndf.show()
ndf1=df.withColumn("rownum",monotonically_increasing_id()+1)
ndf1.show()
ndf2=df.withColumn("Rownum",monotonically_increasing_id()+1).where(col('Rownum')<=8)
ndf2.show(truncate=False)
df.createOrReplaceTempView('temp')
ndf4=spark.sql("select * from temp")
ndf4.show()