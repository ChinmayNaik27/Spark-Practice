#adding days to data
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("Order Date",to_date(col('Order Date'),'d-M-yyyy')).withColumn("Current_date",current_date())\
    .withColumn("DateAdd",date_add(col('Current_date'),100))  #adds 100 days to the column created named current date
ndf.show()