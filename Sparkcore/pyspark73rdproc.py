#creating 4 group for given data by ntile
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.partitionBy('state').orderBy(col('zip').desc())
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/bigdata/DATASETS/us-500.csv")
ndf=df.withColumn('ntile _column',ntile(5).over(win))
ndf.show()
ndf1=df.withColumn('ntile_col',ntile(10).over(win)).where(col('ntile_col')==1)
ndf1.show()

#sql friendly
df.createOrReplaceTempView('tab')
ndf2=spark.sql("select * from tab where zip between 85260 and 85381")
ndf2.show()