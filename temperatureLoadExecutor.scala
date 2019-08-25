/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the temperature observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1960),(1961-2012),
 (2013-2017, manual station),(2013-2017, automatic station)}----*/
/*---------------------------------------------------------------------------------------------*/

package com.implement.spark.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.log4j.Logger
import com.typesafe.config._
import java.io.File
import scala.collection.mutable.ArrayBuffer

class temperatureLoadExecutor(var spark: SparkSession) {
  val logger = Logger.getLogger(this.getClass.getName)
  
  /*-----------------Below snippet is written to pass the 4 values for each dataset from the temperaturePressure.properties file
1.link for the different datasets
2.final temporary table name from which data is loaded to final table
3.create statement of the final table
4.overwrite query for the final table(fields are separated by \n)
--------------------*/
  
  val cfg=ConfigFactory.parseFile(new File("C:/cfg_files/temperaturePressure.properties"))
  var metric1 = ArrayBuffer[String]()
  metric1.appendAll(cfg.getString("metric1").split("\n"))
  var metric2 = ArrayBuffer[String]()
  metric2.appendAll(cfg.getString("metric2").split("\n"))
  var metric3 = ArrayBuffer[String]()
  metric3.appendAll(cfg.getString("metric3").split("\n"))
  var metric4 = ArrayBuffer[String]()
  metric4.appendAll(cfg.getString("metric4").split("\n"))
  var metric5 = ArrayBuffer[String]()
  metric5.appendAll(cfg.getString("metric5").split("\n"))
  
  /*--------------------------------------------------------------------------------------------------*/
        
  def processTemperatureDataset1 {

    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1756 TO 1858")

    def temperatureDataset1_load(): scala.io.BufferedSource = {
      try {
        val temperatureDataset1 = scala.io.Source.fromURL(metric1(0)) 
        temperatureDataset1
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
    //metric1(0),metric1(1),metric1(2),metric1(3) For details refer temperaturePressure.properties file comments
    
   
    val temperatureDataset1 =temperatureDataset1_load()

    val temperatureDatasetRdd1 = spark.sparkContext.parallelize(temperatureDataset1.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1756 TO 1858")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1756 TO 1858")

    val finalTemperatureDatasetRdd1 = temperatureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6))) //0th column not taken because it is blank
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1756 TO 1858")

    val temperatureDatasetDf1 = spark.createDataFrame(finalTemperatureDatasetRdd1, dfSchema1)

    val finalTemperatureDatasetDf1 = temperatureDatasetDf1.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf1.createOrReplaceTempView(metric1(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    spark.sql(metric1(2))

    spark.sql(metric1(3))

    /*Method : 2*/

    // finalTemperatureDatasetDf1.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1756_1858_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
  }

  def processTemperatureDataset2 {
    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1859 TO 1960---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1859 TO 1960")

    def temperatureDataset2_load(): scala.io.BufferedSource = {
      try {
        val temperatureDataset2 = scala.io.Source.fromURL(metric2(0))
        temperatureDataset2
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val temperatureDataset2 =temperatureDataset2_load()
	
	val temperatureDatasetRdd2 = spark.sparkContext.parallelize(temperatureDataset2.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1859 TO 1960")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1859 TO 1960")

    val finalTemperatureDatasetRdd2 = temperatureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8)
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1859 TO 1960")

    val temperatureDatasetDf2 = spark.createDataFrame(finalTemperatureDatasetRdd2, dfSchema2)

    val finalTemperatureDatasetDf2 = temperatureDatasetDf2.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf2.createOrReplaceTempView(metric2(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric2(2))

    spark.sql(metric2(3))

    /*Method : 2*/

    //  finalTemperatureDatasetDf2.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1859_1960_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }

  def processTemperatureDataset3 {
    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1961 TO 2012---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1961 TO 2012")

   def temperatureDataset3_load(): scala.io.BufferedSource = {
      try {
        val temperatureDataset3 = scala.io.Source.fromURL(metric3(0))
        temperatureDataset3
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val temperatureDataset3 =temperatureDataset3_load()
	
	val temperatureDatasetRdd3 = spark.sparkContext.parallelize(temperatureDataset3.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1961 TO 2012")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1961 TO 2012")

    val finalTemperatureDatasetRdd3 = temperatureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1961 TO 2012")

    val temperatureDatasetDf3 = spark.createDataFrame(finalTemperatureDatasetRdd3, dfSchema3)

    val finalTemperatureDatasetDf3 = temperatureDatasetDf3.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf3.createOrReplaceTempView(metric3(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric3(2))

    spark.sql(metric3(3))

    /*Method : 2*/

    // finalTemperatureDatasetDf3.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1961_2012_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
  }

  def processTemperatureDataset4 {

    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file RANGE 2013 TO 2017 FROM MANUAL STATION")

    def temperatureDataset4_load(): scala.io.BufferedSource = {
      try {
        val temperatureDataset4 = scala.io.Source.fromURL(metric4(0))
        temperatureDataset4
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val temperatureDataset4 =temperatureDataset4_load()
	
	val temperatureDatasetRdd4 = spark.sparkContext.parallelize(temperatureDataset4.mkString.split("\n"))

    logger.info("defining schema RANGE 2013 TO 2017 FROM MANUAL STATION")

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

    logger.info("creating final data with data type conversion RANGE 2013 TO 2017 FROM MANUAL STATION")

    val finalTemperatureDatasetRdd4 = temperatureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    logger.info("creating dataframe RANGE 2013 TO 2017 FROM MANUAL STATION")

    val temperatureDatasetDf4 = spark.createDataFrame(finalTemperatureDatasetRdd4, dfSchema4)

    val finalTemperatureDatasetDf4 = temperatureDatasetDf4.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf4.createOrReplaceTempView(metric4(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric4(2))

    spark.sql(metric4(3))

    /*Method : 2*/

    // finalTemperatureDatasetDf4.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_2013_2017_manual_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }

  def processTemperatureDataset5 {
    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    def temperatureDataset5_load(): scala.io.BufferedSource = {
      try {
        val temperatureDataset5 = scala.io.Source.fromURL(metric5(0))
        temperatureDataset5
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val temperatureDataset5 =temperatureDataset5_load()
	
	val temperatureDatasetRdd5 = spark.sparkContext.parallelize(temperatureDataset5.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val finalTemperatureDatasetRdd5 = temperatureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val temperatureDatasetDf5 = spark.createDataFrame(finalTemperatureDatasetRdd5, dfSchema5)

    val finalTemperatureDatasetDf5 = temperatureDatasetDf5.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf5.createOrReplaceTempView(metric5(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric5(2))

    spark.sql(metric5(3))

    /*Method : 2*/

    //   finalTemperatureDatasetDf5.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_2013_2017_auto_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
  }
  /*--------Checking data---------*/

  spark.table("default.temp_obs_1756_1858_parq").show(10, false)
  spark.table("default.temp_obs_1859_1960_parq").show(10, false)
  spark.table("default.temp_obs_1961_2012_parq").show(10, false)
  spark.table("default.temp_obs_2013_2017_manual_parq").show(10, false)
  spark.table("default.temp_obs_2013_2017_auto_parq").show(10, false)

  /*--------------------------------*/

}