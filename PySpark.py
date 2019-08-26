from pyspark import SparkContext

sc = SparkContext(appName="WordCount")

input_file = sc.textFile("hdfs:///user/shakespeare.txt")
#input_file = sc.textFile("file:///home/hadoop/shakespeare.txt") 

b = input_file.flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

#b.coalesce(1).saveAsTextFile("./b.txt")
b.saveAsTextFile("file:///home/hadoop/count_result.txt")