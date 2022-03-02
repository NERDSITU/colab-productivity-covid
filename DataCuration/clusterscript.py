import os
os.environ['SPARK_HOME']='/home/vicp/spark-3.0.3-bin-hadoop2.7'
os.environ['SPARK_LOCAL_DIRS']='/home/vicp/TMP'
os.environ['LOCAL_DIRS']=os.environ['SPARK_LOCAL_DIRS']
os.environ['SPARK_WORKER_DIR']=os.path.join(os.environ['SPARK_LOCAL_DIRS'], 'work')
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64"

from sparkhpc import sparkjob

sparkjob.start_cluster('20000M', 
                       cores_per_executor=4, 
                       spark_home=os.environ['SPARK_HOME'])
