from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def writeTopElements(df):
	top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
	tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
	f = open("top.txt", "x")
	for (a, b) in zip(top_tags, tags_count): 
		print (a, b)



def process_rdd(time, rdd):
	print("----------- %s -----------" % str(time))
	try:
		sql_context = get_sql_context_instance(rdd.context)
		# convert the RDD to Row RDD
		row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
		# create a DF from the Row RDD
		hashtags_df = sql_context.createDataFrame(row_rdd)
		# Register the dataframe as table
		hashtags_df.registerTempTable("hashtags")
		# get the top 10 hashtags from the table using SQL and print them
		hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
		hashtag_counts_df.show()
		writeTopElements(hashtag_counts_df)

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 10)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9008)
# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
tags_totals = hashtags.countByWindow(10*60*60,30)
tags_totals.pprint()
# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()