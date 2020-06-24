import pyspark
import os

os.environ["YARN_CONF_DIR"] = os.getcwd() + "/conf"
os.environ["HADOOP_CONF_DIR"] = os.getcwd() + "/conf"
os.environ["HADOOP_USER_NAME"] = "root"

conf = pyspark.SparkConf()
conf.setAppName("hello3")
conf.setMaster("yarn")
conf.set("spark.submit.deployMode", "client")

sc = pyspark.SparkContext(conf=conf)

data = [1, 2, 3, 4, 5, 8, 9]

myRDD = sc.parallelize(data)

myRDD.collect()

myRDD.count()

print(myRDD)
