# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 16:47:07 2022

@author: Tim Chen
"""
import findspark
findspark.init()
import pyspark
findspark.find()
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("tim").master("local").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", 'true')
spark.sparkContext.setLogLevel(logLevel="ERROR")
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
sqlContext = SQLContext(spark)
data_jan = spark.read.csv('./yellow_tripdata_2009-01.csv', header=True, sep=',')
data_feb = spark.read.csv('./yellow_tripdata_2009-02.csv', header=True, sep=',')
data_mar = spark.read.csv('./yellow_tripdata_2009-03.csv', header=True, sep=',')



#%%
concat_data = data_jan.union(data_feb)
concat_data = concat_data.union(data_mar)
concat_data = concat_data.filter(concat_data.Start_Lon != 0)
concat_data = concat_data.filter(concat_data.Start_Lat != 0)
concat_data = concat_data.filter(concat_data.End_Lon != 0)
concat_data = concat_data.filter(concat_data.End_Lat != 0)
concat_data = concat_data.filter(concat_data.Fare_Amt > 1)
concat_data = concat_data.filter(concat_data.Passenger_Count > 0)
concat_data = concat_data.withColumn('Payment_Type_New', when(concat_data.Payment_Type == 'CASH', 'Cash').when(concat_data.Payment_Type == 'CREDIT', 'Credit').otherwise(concat_data.Payment_Type))
concat_data.show()

#%%
######
# Q1 #
######
#Q1-1
new=concat_data.groupby('Start_Lon','Start_Lat').count().sort('count', ascending=False).show(truncate=False,n=10)


#%%
#Q1-2
concat_data.groupby('End_Lon','End_Lat').count().sort('count', ascending=False).show(truncate=False,n=10)