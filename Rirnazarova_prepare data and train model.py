#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("rirnazarova_spark").getOrCreate()


# reading and trasforming train and test datasets from hdfs
CRM_df=spark.read.csv("RI_finalProject/pipeline_CRM.csv", sep=';', header=True) \
    .withColumn('Customer ID', col('Customer ID').cast(StringType())) \
    .withColumn('Customer Type', col('Customer Type').cast(StringType())) \
    .withColumn('Opportunity ID', col('Opportunity ID').cast(StringType())) \
    .withColumn('Opportunity Creation Date', col('Opportunity Creation Date').cast(StringType())) \
    .withColumn('Opportunity Customer Decision Date', col('Opportunity Customer Decision Date').cast(StringType())) \
    .withColumn('Pipeline Group', col('Pipeline Group').cast(StringType())) \
    .withColumn('Item Origin Country', col('Item Origin Country').cast(StringType())) \
    .withColumn('Item Destination Country', col('Item Destination Country').cast(StringType())) \
    .withColumn('Item Product Group', col('Item Product Group 2').cast(StringType())) \
    .withColumn('Active Pipeline Date', col('Active Pipeline Date').cast(StringType())) \
    .withColumn('Customer Address City', col('Customer Address City').cast(StringType())) \
    .withColumn('Customer Address Country', col('Customer Address Country').cast(StringType())) \
    .withColumn('Customer Global Industry', col('Customer Global Industry').cast(StringType())) \
    .withColumn('Customer Local Industry', col('Customer Local Industry').cast(StringType())) \
    .withColumn('Customer Hierarchy Top Node', col('Customer Hierarchy Top Node').cast(StringType())) \
    .withColumn('Customer Last Activity End Date', col('Customer Last Activity End Date').cast(StringType())) \
    .withColumn('Customer Segment', col('Customer Segment').cast(StringType())) \
    .withColumn('EmpResp Customer Sales Org', col('EmpResp Customer Sales Org').cast(StringType())) \
    .withColumn('EmpResp Item Lane Country', col('EmpResp Item Lane Country').cast(StringType())) \
    .withColumn('EmpResp Item Lane Position Code', col('EmpResp Item Lane Position Code').cast(StringType())) \
    .withColumn('EmpResp Item Lane Position Descr', col('EmpResp Item Lane Position Descr').cast(StringType())) \
    .withColumn('Expected First Shipment Date', col('Expected First Shipment Date').cast(StringType())) \
    .withColumn('Is Customer', col('Is Customer').cast(StringType())) \
    .withColumn('Is Prospect', col('Is Prospect').cast(StringType())) \
    .withColumn('Item Competitor', col('Item Competitor').cast(StringType())) \
    .withColumn('Item Last Modification Date', col('Item Last Modification Date').cast(StringType())) \
    .withColumn('Item Product', col('Item Product').cast(StringType())) \
    .withColumn('Item Status', col('Item Status').cast(StringType())) \
    .withColumn('Item Status Modification Date', col('Item Status Modification Date').cast(StringType())) \
    .withColumn('Item Status Reason', col('Item Status Reason').cast(StringType())) \
    .withColumn('Item Tradelane', col('Item Tradelane').cast(StringType())) \
    .withColumn('Item Type', col('Item Type').cast(StringType())) \
    .withColumn('Opportunity Contract Start Date', col('Opportunity Contract Start Date').cast(StringType())) \
    .withColumn('Opportunity Contract End Date', col('Opportunity Contract End Date').cast(StringType())) \
    .withColumn('Opportunity Sales Stage', col('Opportunity Sales Stage').cast(StringType())) \
    .withColumn('Item Expected Value EUR', col('Item Expected Value').cast(DoubleType())) \
    .withColumn('Item Number of Shipments', col('Item Number of Shipments').cast(IntegerType())) \
    .withColumn('Item Quantity', col('Item Quantity').cast(DoubleType())) \
    .withColumn('Number of Activities', col('Number of Activities').cast(IntegerType())) \
    .withColumn('Duration Days  Preselling', col('Duration Days  Preselling').cast(IntegerType())) \
    .withColumn('Duration Days  Qualified', col('Duration Days  Qualified').cast(IntegerType())) \
    .withColumn('Duration Days  Selling', col('Duration Days  Selling').cast(IntegerType())) \
    .withColumn('Duration Days  Quote', col('Duration Days  Quote').cast(IntegerType())) \
    .withColumn('Duration Days  Contract', col('Duration Days  Contract').cast(IntegerType())) \
    .withColumn('Duration Days  Customer Order Received', col('Duration Days  Customer Order Received').cast(IntegerType())) \
.cache()



CRM_df.show(5)
CRM_df.printSchema()
CRM_df.count()

# preparing fields
CRM_df.select('Customer Type').groupBy('Customer Type').count().show()
CRM_data=CRM_df.withColumn('CustomerType', F.when(col('Customer Type')=="#","P").otherwise(col('Customer Type')))
CRM_data.select('CustomerType').groupBy('CustomerType').count().show()
CRM_data=CRM_data.drop('Customer Type')
CRM_data=CRM_data.drop('Pipeline Group')
CRM_data.printSchema()



