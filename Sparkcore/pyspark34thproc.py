#fetching all names from given group
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
ndf=df.groupBy(col('City')).agg(count(col('*')),collect_list(col('ship mode')))
ndf.show(truncate=False)
