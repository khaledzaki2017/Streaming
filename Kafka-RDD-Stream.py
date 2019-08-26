'''
Create Kafka Topic
	kafka-topics.sh --create --zookeeper localhost:2185 --replication-factor 1 --partitions 1 --topic pyspkaf_test
Start the kafka producer
	kafka-console-producer.sh --broker-list localhost:9092 --topic pyspkaf_test
Start pyspark and include the spark-streaming-kafka pakage in it
	pyspark --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0
'''
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext

def stringToNumberSum(data):
	removedSpaceData = data.strip()
	if removedSpaceData == '' :
		return(None)
	splittedData = removedSpaceData.split(' ')
	numData = [float(x) for x in splittedData]
	sumOfData = sum(numData)
	return (sumOfData)

#Streaming Context with Interval 10 Sec
KStreamContext = StreamingContext(sc, 10)

pyKafStream = KafkaUtils.createDirectStream(ssc = KStreamContext,zkQuorum = 'localhost:2181',groupId = 'pyspkaf_testGroup',topics = {'pyspkaf_test':1})

sumedData = bookKafkaStream.map(lambda data : stringToNumberSum(data[1]))

sumedData.pprint()

KStreamContext.start()

KStreamContext.awaitTerminationOrTimeout(30)