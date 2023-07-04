from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set(access.log))
sc = SparkContext(conf = conf)


errors = logData.filter(lambda line: "ERROR" in line)
