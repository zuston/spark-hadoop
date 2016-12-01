from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkConf(conf=conf)

rdd = sc.textFile('./data/data.txt')

words = rdd.flatMap(lambda x:x.split(" "))


countRdd = words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

for count in countRdd.collect():
    print count[0]+" "+str(count[1])