#obtiaining date in required format

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("OD",to_date(col("Order Date"),'d-M-yyyy')).withColumn("date_format",date_format(col('OD'),'d/M/yyyy/Q/MMM/MMMM/dd/EEEE/E HH:mm:ss'))
ndf.show(truncate=False)