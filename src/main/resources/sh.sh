#不计算变量只是查询后写库
export SPARK_CONF_DIR=/home/hadoop/taskJar/biz_conf
nohup spark-submit --master spark://datacenter17:7077,datacenter18:7077  \
--conf spark.driver.memory=10g \
--conf spark.ui.showConsoleProgress=false \
--class com.lakala.finance.anti_fraud.Graph2Hive \
--total-executor-cores 6  --conf spark.executor.cores=2 --executor-memory 16g \
~/taskJar/anti-fraud20170810.jar ~/taskJar/biz_conf > \
~/log/Graph2Hive20170810 &


#伪实时计算变量
export SPARK_CONF_DIR=/home/hadoop/taskJar/biz_conf
nohup spark-submit --master spark://datacenter17:7077,datacenter18:7077  \
--conf spark.driver.memory=30g \
--conf spark.ui.showConsoleProgress=false \
--driver-class-path /home/hadoop/taskJar/mysql-connector-java-5.1.37.jar \
--jars /home/hadoop/taskJar/mysql-connector-java-5.1.37.jar \
--class com.lakala.finance.anti_fraud.Graph2Var \
--total-executor-cores 69   --executor-memory 40g \
~/taskJar/anti-fraud20170810.jar ~/taskJar/biz_conf  \
> ~/log/Graph2var20170810 &

--driver-java-options "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false" \


--driver-java-options "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"

spark-shell --master spark://datacenter17:7077,datacenter18:7077 --conf spark.executor.cores=6
