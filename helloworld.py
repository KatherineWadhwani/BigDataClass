from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)


logData = access(logFile).cache()
errors = logData.filter(lambda line: "ERROR" in line)
