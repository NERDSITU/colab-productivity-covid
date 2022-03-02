# -*- coding: utf-8 -*-
"""
@author: lasse
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from MAG import MicrosoftAcademicGraph
import os
from sparkhpc import sparkjob
import findspark

# set environment variables
os.environ["SPARK_LOCAL_DIRS"] = "/home/vicp/TMP"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64"
os.environ['SPARK_HOME'] = "/home/vicp/spark-3.0.3-bin-hadoop2.7"

def get_cluster_client(jobid, memory_per_executor=16000): # 16000

    sj = sparkjob.sparkjob(jobid=jobid, memory_per_executor=memory_per_executor)
    config_options = {
    "spark.memory.offHeap.enabled": True,
    "spark.memory.offHeap.size":"2g",
    "spark.sql.adaptive.enabled": True,
    "spark.sql.adaptive.coalescePartitions.enabled": True,
    "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly": False,
    "spark.shuffle.io.retryWait": "60s",
    "spark.reducer.maxReqsInFlight": 5,
    "spark.executor.memoryOverhead": "2gb",
    "spark.driver.memory": "20g",
    "spark.sql.shuffle.partitions": 300,
    "spark.sql.broadcastTimeout": "36000"
    }

    job = sj.start_spark(extra_conf = config_options)
    spark = SparkSession.builder.config(conf=job.getConf()).getOrCreate()
    return spark

def get_node_client(executor_memory="24g"): # 2g driver memory before.

    spark = SparkSession \
      .builder \
      .config("spark.executor.memory", executor_memory)\
      .config("spark.driver.memory", "2g")\
      .config("spark.executor.cores", 7)\
      .config("spark.memory.offHeap.enabled", True)\
      .config("spark.memory.offHeap.size","2g")\
      .config("spark.sql.adaptive.enabled", True)\
      .config("spark.sql.adaptive.coalescePartitions.enabled", True)\
      .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", False)\
      .appName("MAG app") \
      .getOrCreate()

    return spark


def get_mag_with_cluster_connection(jobid, memory_per_executor=16000, # 16000
                                    data_folderpath="/home/vicp/data/2021-08-02/"):

    spark = get_cluster_client(jobid, memory_per_executor=memory_per_executor)
    mag = MicrosoftAcademicGraph(spark=spark, data_folderpath=data_folderpath)

    return mag, spark
