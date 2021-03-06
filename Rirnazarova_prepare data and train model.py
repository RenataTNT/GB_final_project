#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType, DoubleType, DateType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, isnan, udf
from pyspark.sql.functions import to_date, month, year, date_add


spark = SparkSession.builder.appName("rirnazarova_spark").getOrCreate()


# reading and trasforming train and test datasets from hdfs
CRM_df=spark.read.csv("RI_finalProject/pipeline_CRM.csv", sep=';', header=True) \
    .withColumn('Customer ID', col('Customer ID').cast(StringType())) \
    .withColumn('Customer Type', col('Customer Type').cast(StringType())) \
    .withColumn('Opportunity ID', col('Opportunity ID').cast(IntegerType())) \
    .withColumn('Opportunity Creation Date', to_date(col('Opportunity Creation Date'), "dd.MM.yyyy")) \
    .withColumn('Opportunity Customer Decision Date', to_date(col('Opportunity Customer Decision Date'), "dd.MM.yyyy"))\
    .withColumn('Pipeline Group', col('Pipeline Group').cast(StringType())) \
    .withColumn('Item Origin Country', col('Item Origin Country').cast(StringType())) \
    .withColumn('Item Destination Country', col('Item Destination Country').cast(StringType())) \
    .withColumn('Item Product Group 2', col('Item Product Group 2').cast(StringType())) \
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
    .withColumn('Item Status Modification Date', to_date(col('Item Status Modification Date'), "dd.MM.yyyy")) \
    .withColumn('Item Status Reason', col('Item Status Reason').cast(StringType())) \
    .withColumn('Item Tradelane', col('Item Tradelane').cast(StringType())) \
    .withColumn('Item Type', col('Item Type').cast(StringType())) \
    .withColumn('Opportunity Contract Start Date', col('Opportunity Contract Start Date').cast(StringType())) \
    .withColumn('Opportunity Contract End Date', col('Opportunity Contract End Date').cast(StringType())) \
    .withColumn('Opportunity Sales Stage', col('Opportunity Sales Stage').cast(StringType())) \
    .withColumn('Item Expected Value', col('Item Expected Value').cast(DoubleType())) \
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

#analyze fields
CRM_df.select('Customer Type').groupBy('Customer Type').count().show()
CRM_df.select(year('Opportunity Customer Decision Date').alias('Decision year')).groupBy('Decision year').count().show()
CRM_df.select('Item Origin Country').groupBy('Item Origin Country').count().show()

#target column
CRM_df.select('Item Status').groupBy('Item Status').count().show()

# clean fields

CRM_data=CRM_df.withColumn('CustomerType', F.when(col('Customer Type')=="#","P").otherwise(col('Customer Type')))
#CRM_data=CRM_df.withColumn('Origin Country', F.when(col('EmpResp Item Lane Country')=="#","EmpResp Item Lane Country").otherwise(col('EmpResp Item Lane Country')))
CRM_data=CRM_data.drop('Customer Type')
CRM_data=CRM_data.drop('Pipeline Group')
CRM_data = CRM_data.drop('Active Pipeline Date')
CRM_data  = CRM_data.withColumn('Customer Decision Date', F.when(col('Opportunity Customer Decision Date').isNull() | (year('Opportunity Customer Decision Date') > 2050), date_add(col('Opportunity Creation Date'),90))\
    .otherwise(col('Opportunity Customer Decision Date')))
CRM_data  = CRM_data.withColumn('Customer Decision M_Y', F.concat_ws("_", month(col('Customer Decision Date')),year(col('Customer Decision Date'))))
CRM_data = CRM_data.drop('Opportunity Customer Decision Date')
CRM_data = CRM_data.drop('Customer Decision Date')

CRM_data  = CRM_data.withColumn('Customer Creation M_Y', F.concat_ws("_", month(col('Opportunity Creation Date')),year(col('Opportunity Creation Date'))))
CRM_data = CRM_data.drop('Opportunity Creation Date')

CRM_data  = CRM_data.withColumn('Item Status Modification Date M_Y', F.concat_ws("_", month(col('Item Status Modification Date')),year(col('Item Status Modification Date'))))
CRM_data = CRM_data.drop('Item Status Modification Date')

CRM_data=CRM_data.withColumn('Status', F.when((col('Item Status')!="Won") & (col('Item Status')!="In Progress"),"Lost").otherwise(col('Item Status')))
CRM_data = CRM_data.drop('Item Status')

CRM_data = CRM_data.drop('Item Status Reason')
CRM_data = CRM_data.drop('Opportunity Sales Stage')
CRM_data = CRM_data.drop('Duration Days  Customer Order Received')

CRM_data=CRM_data.na.fill(0,subset=["Duration Days  Preselling", "Duration Days  Qualified", "Duration Days  Selling", "Duration Days  Quote", "Duration Days  Contract"])

#new features

HitRatePivot=CRM_data.groupBy('EmpResp Customer Sales Org','Item Product','EmpResp Item Lane Position Descr','Customer Local Industry' )\
    .pivot("Status").sum('Item Expected Value')
HitRatePivot.show()
HitRatePivot=HitRatePivot.na.fill(1,['In Progress','Lost','Won'])

HitRatePivot=HitRatePivot.withColumn('HitRate', F.round(col('Won')/(col("Won")+col('Lost')),2))

CRM_data_final=CRM_data.join(HitRatePivot,on=['EmpResp Customer Sales Org','Item Product','EmpResp Item Lane Position Descr','Customer Local Industry'],how='left')

CRM_data_final.printSchema()

#builing model

data=CRM_data_final.where(col("Status") !="In Progress").cache()
data.count()
data.columns



data=data.withColumn("chance", F.when(col("Status")=="Won",1).otherwise(0))
data=data.drop("Status")
data.select('chance').groupBy('chance').count().show()

data.printSchema()


# for c in data.columns:
#     data.select(c).where(col(c).isNull() | isnan(c) ).show()

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

stages = []

# label_stringIdx

label_stringIdx = StringIndexer(inputCol = 'chance', outputCol = 'label', handleInvalid = 'keep')
stages += [label_stringIdx]

# stringIndexer
categoricalColumns = ['EmpResp Customer Sales Org', 'Item Product', 'EmpResp Item Lane Position Descr', 'Customer Local Industry', \
                      'Customer ID', 'Item Origin Country', 'Item Destination Country', \
                      'Item Product Group 2', 'Customer Address City', 'Customer Address Country', 'Customer Global Industry', 'Customer Hierarchy Top Node', \
                      'Customer Last Activity End Date', 'Customer Segment', 'EmpResp Item Lane Country', 'EmpResp Item Lane Position Code', \
                      'Expected First Shipment Date', 'Is Customer', 'Is Prospect', 'Item Competitor', 'Item Last Modification Date', \
                       'Item Tradelane', 'Item Type', 'Opportunity Contract Start Date', 'Opportunity Contract End Date',   \
                      'CustomerType', 'Customer Decision M_Y', 'Customer Creation M_Y', 'Item Status Modification Date M_Y']




num_columns=['Opportunity ID','Item Number of Shipments', 'Item Quantity', 'Number of Activities', 'Duration Days  Preselling', 'Duration Days  Qualified', 'Duration Days  Selling', \
                      'Duration Days  Quote', 'Duration Days  Contract', 'Item Expected Value','In Progress', 'Lost', 'Won', 'HitRate']



for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol,
                                  outputCol = categoricalCol + 'Index',
                                  handleInvalid = 'keep')
    encoder = OneHotEncoder(inputCol=stringIndexer.getOutputCol(),
                            outputCol=categoricalCol + "classVec")
    stages += [stringIndexer, encoder]

#assembler features

assemblerInputs = [c + "classVec" for c in categoricalColumns] + num_columns

assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features").setHandleInvalid("keep")
stages += [assembler]

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

trainingData.count()

# Train a GBT model.
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
stages += [gbt]


# Chain indexers and GBT in a Pipeline
pipeline = Pipeline(stages=stages)

# train model
model = pipeline.fit(trainingData)


#сохраняем модель на HDFS
model.write().overwrite().save("rirnazarova_gbt_crm3")


from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


crm_model = PipelineModel.load("rirnazarova_gbt_crm3")

# делаем предсказания на тренировочной выборке
predictions_train = crm_model.transform(trainingData)
print('Test Area Under ROC for Train data', evaluator.evaluate(predictions_train))

# делаем предсказания на тестовой выборке
predictions = crm_model.transform(testData)
print('Test Area Under ROC for Test data', evaluator.evaluate(predictions))


#оценка качества модели
f1=MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='f1')
accuracy=MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')

#accuracy on train and test
print('Train accuracy', accuracy.evaluate(predictions_train))
print('Test accuracy', accuracy.evaluate(predictions))



TP=predictions.select('Customer ID').where((F.col('prediction')==1) & (F.col('label')==1)).count()
TN=predictions.select('Customer ID').where((F.col('prediction')==0) & (F.col('label')==0)).count()


c1=predictions.select('Customer ID').where(F.col('label')==1).count()
Recall=float(TP)/float(c1)
Recall

FP=predictions.select('Customer ID').where((F.col('prediction')==1) & (F.col('label')==0)).count()
Precision=float(TP)/float(TP+FP)
Precision

f1.evaluate(predictions)


#выделение основных признаков, влияющих на предсказание

gb_data=crm_model.stages[-1]

def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    return(list_extract)

pred_df= crm_model.transform(testData)


l=[x["name"] for x in sorted(pred_df.schema["features"].metadata["ml_attr"]["attrs"]["binary"] \
                             +pred_df.schema["features"].metadata["ml_attr"]["attrs"]["numeric"], \
                             key=lambda x: x["idx"])]

fimp=gb_data.featureImportances
#print(fimp)
f2=list(fimp)
FI_list=dict(zip(l,f2))
sorter_FI=list(sorted(FI_list.items(), key=lambda kv: kv[1]))
best_features=sorter_FI[-15:]

BF=[]
for item in best_features:
    item=list(item)
    item[0]=str(item[0])
    item[1] = str(item[1])
    BF.append(item)

BFschema = StructType() \
    .add("feature", StringType()) \
    .add("importance", StringType())

best_featuresDF=spark.createDataFrame(data=BF, schema=BFschema)
best_featuresDF.sort('importance',ascending=False).show()



# Предсказпния на валидационной выборке
prospect_data=CRM_data_final.where(col("Status") =="In Progress").cache()
prospect_data.count()

prospect_data=prospect_data.drop("Status")
predictions_prospect = crm_model.transform(prospect_data)


#преобразование вектора с вероятностями в массив
def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()
    return F.udf(to_array_, ArrayType(DoubleType())).asNondeterministic()(col)

predictions_prospect=predictions_prospect.withColumn('proba', to_array(col('probability')))

#выбираем вероятности для 1-го класса
predictions_prospect=predictions_prospect.withColumn('Chance', col('proba')[1])
predictions_prospect.select('prediction','Chance').show()

#финальный датасет
prospect_data_final=prospect_data.join(predictions_prospect \
                                       .select('Opportunity ID', 'prediction', 'Chance'),on=['Opportunity ID'],how='left')

#сохраняем на hdfs

prospect_data_final.write.option('mode','overwrite'). \
    option("header","true").csv('RI_finalProject/prospect_data_final.csv')

