appname="data_load"
current_time=$(date +"%Y%m%d%H%M%S")
log_path={edge_node_path}
log_file=$appname"_"$current_time.log

export SPARK_MAJOR_VERSION=2

spark-submit --files {edge_node_path}/log4j.properties,{edge_node_path}/hive-site.xml,{edge_node_path}/temperaturePressure.properties --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:{edge_node_path}/log4j.properties' --conf spark.executor.extraJavaOptions='-Dlog4j.configuration=log4j.properties'  --master yarn  --class com.implement.spark.sparkdemo.sparkStartAndProcessor {jar_name} $log_path/$log_file  2>&1

if [ $? -ne 0 ]; then
           echo "The Job got failed.Please check logs." | mailx -s "Status of Temperature and Pressure Observation data Load" abc@gmail.com
		   exit
fi 

echo "The Job got completed Successfully" | mailx -s "Status of Temperature and Pressure Observation data Load" abc@gmail.com
