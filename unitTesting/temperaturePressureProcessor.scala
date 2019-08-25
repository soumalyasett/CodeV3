/*------------------The below code consists of the functions for populating both temperature 
 and pressure tables------------------*/
package com.implement.spark.sparkdemo.sparkUnitTest

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }

class temperaturePressureProcessor {
  
   val spark = SparkSession.builder()
  .master("local")
  .appName("Temperature observation load")
  .config("spark.sql.catalogImplementation","hive")
  .getOrCreate()
  
  
  def processTemperatureDataset1() : Long = {

    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/


    val temperatureDataset1 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd1 = spark.sparkContext.parallelize(temperatureDataset1)


    val dfSchema1 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalTemperatureDatasetRdd1 = temperatureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6))) //0th column not taken because it is blank
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))


    val temperatureDatasetDf1 = spark.createDataFrame(finalTemperatureDatasetRdd1, dfSchema1)

    val finalTemperatureDatasetDf1 = temperatureDatasetDf1.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf1.createOrReplaceTempView("finalTemperatureDatasetDf1Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    spark.sql("""CREATE TABLE if not exists default.temp_obs_1756_1858_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double) stored as parquet""")

    spark.sql("insert overwrite table default.temp_obs_1756_1858_parq select * from finalTemperatureDatasetDf1Table")

    /*Method : 2*/

    // finalTemperatureDatasetDf1.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1756_1858_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
  
    val count1=spark.table("default.temp_obs_1756_1858_parq").count
    count1
    }

  def processTemperatureDataset2(): Long = {
    
 val temperatureDataset2 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd2 = spark.sparkContext.parallelize(temperatureDataset2)


    val dfSchema2 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalTemperatureDatasetRdd2 = temperatureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8)
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble))


    val temperatureDatasetDf2 = spark.createDataFrame(finalTemperatureDatasetRdd2, dfSchema2)

    val finalTemperatureDatasetDf2 = temperatureDatasetDf2.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf2.createOrReplaceTempView("finalTemperatureDatasetDf2Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists  default.temp_obs_1859_1960_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double) stored as parquet""")

    spark.sql("insert overwrite table default.temp_obs_1859_1960_parq select * from finalTemperatureDatasetDf2Table")

    /*Method : 2*/

    //  finalTemperatureDatasetDf2.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1859_1960_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

      val count2=spark.table("default.temp_obs_1859_1960_parq").count
    count2
  
  }

  def processTemperatureDataset3() : Long = {
    
     /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1961 TO 2012---------------------------------*/


    val temperatureDataset3 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd3 = spark.sparkContext.parallelize(temperatureDataset3)


    val dfSchema3 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalTemperatureDatasetRdd3 = temperatureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))


    val temperatureDatasetDf3 = spark.createDataFrame(finalTemperatureDatasetRdd3, dfSchema3)

    val finalTemperatureDatasetDf3 = temperatureDatasetDf3.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf3.createOrReplaceTempView("finalTemperatureDatasetDf3Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists  default.temp_obs_1961_2012_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double, TempMeanC double) stored as parquet""")

    spark.sql("insert overwrite table default.temp_obs_1961_2012_parq select * from finalTemperatureDatasetDf3Table")

    /*Method : 2*/

    // finalTemperatureDatasetDf3.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1961_2012_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
 val count3=spark.table("default.temp_obs_1961_2012_parq").count
    count3
  
  }
  
  def processTemperatureDataset4() : Long = {
  /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION---------------------------------*/


    val temperatureDataset4 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd4 = spark.sparkContext.parallelize(temperatureDataset4)


    val dfSchema4 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalTemperatureDatasetRdd4 = temperatureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))


    val temperatureDatasetDf4 = spark.createDataFrame(finalTemperatureDatasetRdd4, dfSchema4)

    val finalTemperatureDatasetDf4 = temperatureDatasetDf4.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf4.createOrReplaceTempView("finalTemperatureDatasetDf4Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists  default.temp_obs_2013_2017_manual_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double, TempMeanC double) stored as parquet""")

    spark.sql("insert overwrite table default.temp_obs_2013_2017_manual_parq select * from finalTemperatureDatasetDf4Table")

    /*Method : 2*/

    // finalTemperatureDatasetDf4.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_2013_2017_manual_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    
    val count4=spark.table("default.temp_obs_2013_2017_manual_parq").count
    count4
}
  
   def processTemperatureDataset5() : Long =  {
     
  /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION---------------------------------*/


    val temperatureDataset5 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd5 = spark.sparkContext.parallelize(temperatureDataset5)


    val dfSchema5 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalTemperatureDatasetRdd5 = temperatureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))


    val temperatureDatasetDf5 = spark.createDataFrame(finalTemperatureDatasetRdd5, dfSchema5)

    val finalTemperatureDatasetDf5 = temperatureDatasetDf5.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf5.createOrReplaceTempView("finalTemperatureDatasetDf5Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.temp_obs_2013_2017_auto_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double, TempMeanC double) stored as parquet""")

    spark.sql("insert overwrite table default.temp_obs_2013_2017_auto_parq select * from finalTemperatureDatasetDf5Table")

    /*Method : 2*/

    //   finalTemperatureDatasetDf5.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_2013_2017_auto_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    
    val count5=spark.table("default.temp_obs_2013_2017_auto_parq").count
    count5
}
   
  def processPressureDataset1() : Long ={
     
     /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/


    val pressureDataset1 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1756_1858.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd1 = spark.sparkContext.parallelize(pressureDataset1)


    val dfSchema1 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureIn29_69mm", DoubleType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonPressureIn29_69mm", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningPressureIn29_69mm", DoubleType, true),
        StructField("EveningTempC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd1 = pressureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))


    val pressureDatasetDf1 = spark.createDataFrame(finalPressureDatasetRdd1, dfSchema1)

    val finalPressureDatasetDf1 = pressureDatasetDf1.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf1.createOrReplaceTempView("finalPressureDatasetDf1Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1756_1858_parq(
     Year int, Month int, Day int, MorningPressureInMm29_69 double, MorningTempC double, NoonPressureInMm29_69 double, NoonTempC double, EveningPressureInMm29_69 double, EveningTempC double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_1756_1858_parq select * from finalPressureDatasetDf1Table")

    /*Method : 2*/

    // finalPressureDatasetDf1.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1756_1858_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    
    val count6=spark.table("default.pressure_obs_1756_1858_parq").count
    count6
   }
  
  def processPressureDataset2() : Long = {
/*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1859 TO 1861---------------------------------*/


    val pressureDataset2 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1859_1861.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd2 = spark.sparkContext.parallelize(pressureDataset2)


    val dfSchema2 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureIn2_969mm", DoubleType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("MorningPressureIn2_969mmAt0C", DoubleType, true),
        StructField("NoonPressureIn2_969mm", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("NoonPressureIn2_969mmAt0C", DoubleType, true),
        StructField("EveningPressureIn2_969mm", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("EveningPressureIn2_969mmAt0C", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd2 = pressureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8), r.split(",")(9), r.split(",")(10), r.split(",")(11)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble, a(9).toDouble, a(10).toDouble, a(11).toDouble))


    val pressureDatasetDf2 = spark.createDataFrame(finalPressureDatasetRdd2, dfSchema2)

    val finalPressureDatasetDf2 = pressureDatasetDf2.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf2.createOrReplaceTempView("finalPressureDatasetDf2Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1859_1861_parq(
     Year int, Month int, Day int, MorningPressureIn2_969mm double, MorningTempC double, MorningPressureIn2_969mmAt0C double, NoonPressureIn2_969mm double, NoonTempC double, NoonPressureIn2_969mmAt0C double, EveningPressureIn2_969mm double, EveningTempC double, EveningPressureIn2_969mmAt0C double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_1859_1861_parq select * from finalPressureDatasetDf2Table")

    /*Method : 2*/

    // finalPressureDatasetDf2.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1859_1861_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    
    val count7=spark.table("default.pressure_obs_1859_1861_parq").count
    count7
}
  
  def processPressureDataset3() : Long = {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1862 TO 1937---------------------------------*/


    val pressureDataset3 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1862_1937.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd3 = spark.sparkContext.parallelize(pressureDataset3)


    val dfSchema3 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInMmHg", DoubleType, true),
        StructField("NoonPressureInMmHg", DoubleType, true),
        StructField("EveningPressureInMmHg", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd3 = pressureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))


    val pressureDatasetDf3 = spark.createDataFrame(finalPressureDatasetRdd3, dfSchema3)

    val finalPressureDatasetDf3 = pressureDatasetDf3.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf3.createOrReplaceTempView("finalPressureDatasetDf3Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1862_1937_parq(
     Year int, Month int, Day int, MorningPressureInMmHg double, NoonPressureInMmHg double, EveningPressureInMmHg double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_1862_1937_parq select * from finalPressureDatasetDf3Table")

    /*Method : 2*/

    //.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1862_1937_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    val count8=spark.table("default.pressure_obs_1862_1937_parq").count
    count8
  }
  
  def processPressureDataset4() : Long = {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1938 TO 1960---------------------------------*/


    val pressureDataset4 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1938_1960.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd4 = spark.sparkContext.parallelize(pressureDataset4)


    val dfSchema4 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd4 = pressureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))


    val pressureDatasetDf4 = spark.createDataFrame(finalPressureDatasetRdd4, dfSchema4)

    val finalPressureDatasetDf4 = pressureDatasetDf4.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf4.createOrReplaceTempView("finalPressureDatasetDf4Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1938_1960_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_1938_1960_parq select * from finalPressureDatasetDf4Table")

    /*Method : 2*/

    //finalPressureDatasetDf4.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1938_1960_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    val count9=spark.table("default.pressure_obs_1938_1960_parq").count
    count9
  }
  
  def processPressureDataset5() : Long = {
    
    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1961 TO 2012---------------------------------*/


    val pressureDataset5 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1961_2012.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd5 = spark.sparkContext.parallelize(pressureDataset5)


    val dfSchema5 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd5 = pressureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))


    val pressureDatasetDf5 = spark.createDataFrame(finalPressureDatasetRdd5, dfSchema5)

    val finalPressureDatasetDf5 = pressureDatasetDf5.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf5.createOrReplaceTempView("finalPressureDatasetDf5Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1961_2012_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_1961_2012_parq select * from finalPressureDatasetDf5Table")

    /*Method : 2*/

    // finalPressureDatasetDf5.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1961_2012_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    val count10=spark.table("default.pressure_obs_1961_2012_parq").count
    count10
  }
  
  def processPressureDataset6() : Long = {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION---------------------------------*/


    val pressureDataset6 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_2013_2017.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd6 = spark.sparkContext.parallelize(pressureDataset6)


    val dfSchema6 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd6 = pressureDatasetRdd6.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))


    val pressureDatasetDf6 = spark.createDataFrame(finalPressureDatasetRdd6, dfSchema6)

    val finalPressureDatasetDf6 = pressureDatasetDf6.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf6.createOrReplaceTempView("finalPressureDatasetDf6Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_2013_2017_manual_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_2013_2017_manual_parq select * from finalPressureDatasetDf6Table")

    /*Method : 2*/

    //finalPressureDatasetDf6.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_2013_2017_manual_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    val count11=spark.table("default.pressure_obs_2013_2017_manual_parq").count
    count11
  }
  
  def processPressureDataset7() : Long = {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION---------------------------------*/


    val pressureDataset7 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholmA_barometer_2013_2017.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd7 = spark.sparkContext.parallelize(pressureDataset7)


    val dfSchema7 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/


    val finalPressureDatasetRdd7 = pressureDatasetRdd7.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))


    val pressureDatasetDf7 = spark.createDataFrame(finalPressureDatasetRdd7, dfSchema7)

    val finalPressureDatasetDf7 = pressureDatasetDf7.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf7.createOrReplaceTempView("finalPressureDatasetDf7Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_2013_2017_auto_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet""")

    spark.sql("insert overwrite table default.pressure_obs_2013_2017_auto_parq select * from finalPressureDatasetDf7Table")

    /*Method : 2*/

    // finalPressureDatasetDf7.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_2013_2017_auto_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
  
    val count12=spark.table("default.pressure_obs_2013_2017_manual_parq").count
    count12
  }
}