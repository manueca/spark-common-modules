#################################################################################################
#                                                                                               #
#                   1. run multiple writes in spark parallely                                   #
#               Author  : jerry Cheruvathoor                                                    #
#               Date    : 08/08/2019                                                            #
#===============================================================================================#
import threading
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
def task_ds(sc,val):
    df1=sc.sql('select * from tableb')
    df1.write.mode("overwrite").save('s3://jerry-test/jcher2/spark-testing_1/')
def task_ds1(sc,val):
    df2=sc.sql('select * from tablea"')
    df2.write.mode("overwrite").save('s3://jerry-test/jcher2/spark-testing_2/')
def run_multiple_jobs():
  conf = SparkConf().setAppName('appname')
  conf.set('spark.scheduler.mode', 'FAIR')
  sc= SparkSession.builder.enableHiveSupport() \
        .appName("Loading dtc_doms_inventory_daily") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
  t1 = threading.Thread(target=task_ds, args=(sc,1))
  t1.start()
  t2 = threading.Thread(target=task_ds1, args=(sc,1))
  t2.start()

run_multiple_jobs()