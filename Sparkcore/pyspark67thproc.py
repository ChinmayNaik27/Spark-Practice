#window
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)
# df.printSchema()
win=Window.orderBy(col('Profit').desc())
fin=df.withColumn("rnk",rank().over(win)).orderBy(col('sal').asc_nulls_first())
fin.show()