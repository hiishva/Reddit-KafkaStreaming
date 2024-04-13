from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, ArrayType
from kafka import KafkaProducer
import nltk
import json
from nltk import Tree
from nltk import word_tokenize, pos_tag, ne_chunk

#Downloads for nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
nltk.download('all')

#Initialize spark
spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer = lambda v: json.dumps(v).encode('utf-8'))

#Read from kafka
lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "topic1")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

#Extract the Named Entities
def extractNamedEnt(text):
        words = word_tokenize(text)
        posTags = pos_tag(words)
        namedEnts = ne_chunk(posTags)
        ents = []
        for chunk in namedEnts:
            if isinstance(chunk, Tree) and hasattr(chunk, 'label'):
                trm = ' '.join([w for w, _ in chunk.leaves()])
                namedEntCat = chunk.label()
                ents.append(trm)
        return ents
    
extractNamedEntUdf = spark.udf.register("extractNameEnt", extractNamedEnt, ArrayType(StringType()))


lines = lines.withColumn("namedEntities", explode(extractNamedEntUdf("value")))
namedEntCnt = lines.groupBy("namedEntities").count().orderBy("count", ascending=False).limit(10)

query = namedEntCnt\
        .writeStream\
        .outputMode('complete')\
        .foreachBatch(lambda batchDF, batchId: sendToKafka(batchDF,batchId)) \
        .trigger(processingTime='10 seconds')\
        .start()

def sendToKafka(batchDf, batchId):
    ne = batchDf.collect()
    for row in ne:
        message = {'namedEntity': row.namedEntities,'count':row['count']}
        producer.send('topic2',value=message)

query.awaitTermination()

