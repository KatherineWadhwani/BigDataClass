from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set(clusterproj5))
sc = SparkContext(conf = conf)


errors = logData.filter(lambda line: "ERROR" in line)
