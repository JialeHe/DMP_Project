# spark-submit
spark-submit \
/home/jar/DMP_Project-1.0-SNAPSHOT.jar \
--master local \
--num-executors 2 \
--driver-memory 1G \
--executor-memory 2G \
--executor-cores 2 \
--class com.hjl.scheduler.report.media.NetWorkReport