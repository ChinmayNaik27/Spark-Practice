#changing a columns data type
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("Quantity",col('Quantity').cast(IntegerType())) #changing esxisting column datatype to integer
ndf.show()
df.printSchema()
ndf.printSchema()