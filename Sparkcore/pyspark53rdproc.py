#getting date of friday from order date column from last date
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)
ndf=df.withColumn("OD",to_date(col('Order Date'),'d-M-yyyy')).withColumn("ldate",last_day(col('OD')))\
    .withColumn("monfri",next_day(date_add(last_day(col('OD')),-7),'Fri'))

ndf.show(truncate=False)

