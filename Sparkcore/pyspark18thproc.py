from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
#sepcifying first line as header and sep char to ;
data=spark.read.format("csv").option('sep',';').option('header','true').load("C:\\Users\\chinm\\Downloads\\bank-full.csv")
data.show()