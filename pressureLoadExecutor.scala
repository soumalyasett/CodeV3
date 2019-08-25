/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the pressure observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1861),(1862-1937),
 (1938-1960),(1961-2012),(2013-2017,manual station),(2013-2017,automatic station)}----*/
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

class pressureLoadExecutor(var spark: SparkSession) {
  val logger = Logger.getLogger(this.getClass.getName)
  
   /*-----------------Below snippet is written to pass the 4 values for each dataset from the temperaturePressure.properties file
1.link for the different datasets
2.final temporary table name from which data is loaded to final table
3.create statement of the final table
4.overwrite query for the final table(fields are separated by \n)
--------------------*/
  
  val cfg=ConfigFactory.parseFile(new File("C:/cfg_files/temperaturePressure.properties"))
  var metric6 = ArrayBuffer[String]()
  metric6.appendAll(cfg.getString("metric6").split("\n"))
  var metric7 = ArrayBuffer[String]()
  metric7.appendAll(cfg.getString("metric7").split("\n"))
  var metric8 = ArrayBuffer[String]()
  metric8.appendAll(cfg.getString("metric8").split("\n"))
  var metric9 = ArrayBuffer[String]()
  metric9.appendAll(cfg.getString("metric9").split("\n"))
  var metric10 = ArrayBuffer[String]()
  metric10.appendAll(cfg.getString("metric10").split("\n"))
  var metric11 = ArrayBuffer[String]()
  metric11.appendAll(cfg.getString("metric11").split("\n"))
  var metric12 = ArrayBuffer[String]()
  metric12.appendAll(cfg.getString("metric12").split("\n"))
  
   /*--------------------------------------------------------------------------------------------------*/
  

  def processPressureDataset1 {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1756 TO 1858")

    def pressureDataset1_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset1 = scala.io.Source.fromURL(metric6(0))
        pressureDataset1
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset1 =pressureDataset1_load()
	
	val pressureDatasetRdd1 = spark.sparkContext.parallelize(pressureDataset1.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1756 TO 1858")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1756 TO 1858")

    val finalPressureDatasetRdd1 = pressureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1756 TO 1858")

    val pressureDatasetDf1 = spark.createDataFrame(finalPressureDatasetRdd1, dfSchema1)

    val finalPressureDatasetDf1 = pressureDatasetDf1.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf1.createOrReplaceTempView(metric6(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric6(2))

    spark.sql(metric6(3))

    /*Method : 2*/

    // finalPressureDatasetDf1.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1756_1858_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }
  def processPressureDataset2 {
    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1859 TO 1861---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1859 TO 1861")

    def pressureDataset2_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset2 = scala.io.Source.fromURL(metric7(0))
        pressureDataset2
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset2 =pressureDataset2_load()
	
	val pressureDatasetRdd2 = spark.sparkContext.parallelize(pressureDataset2.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1859 TO 1861")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1859 TO 1861")

    val finalPressureDatasetRdd2 = pressureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8), r.split(",")(9), r.split(",")(10), r.split(",")(11)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble, a(9).toDouble, a(10).toDouble, a(11).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1859 TO 1861")

    val pressureDatasetDf2 = spark.createDataFrame(finalPressureDatasetRdd2, dfSchema2)

    val finalPressureDatasetDf2 = pressureDatasetDf2.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf2.createOrReplaceTempView(metric7(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric7(2))

    spark.sql(metric7(3))

    /*Method : 2*/

    // finalPressureDatasetDf2.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1859_1861_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }

  def processPressureDataset3 {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1862 TO 1937---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1862 TO 1937")

    def pressureDataset3_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset3 = scala.io.Source.fromURL(metric8(0))
        pressureDataset3
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset3 =pressureDataset3_load()
	
	val pressureDatasetRdd3 = spark.sparkContext.parallelize(pressureDataset3.mkString.split("\n"))
	
    logger.info("defining schema FOR THE RANGE 1862 TO 1937")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1862 TO 1937")

    val finalPressureDatasetRdd3 = pressureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1862 TO 1937")

    val pressureDatasetDf3 = spark.createDataFrame(finalPressureDatasetRdd3, dfSchema3)

    val finalPressureDatasetDf3 = pressureDatasetDf3.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf3.createOrReplaceTempView(metric8(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric8(2))

    spark.sql(metric8(3))

    /*Method : 2*/

    //.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1862_1937_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }

  def processPressureDataset4 {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1938 TO 1960---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1938 TO 1960")

    def pressureDataset4_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset4 = scala.io.Source.fromURL(metric9(0))
        pressureDataset4
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset4 =pressureDataset4_load()
	
	val pressureDatasetRdd4 = spark.sparkContext.parallelize(pressureDataset4.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1938 TO 1960")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1938 TO 1960")

    val finalPressureDatasetRdd4 = pressureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1938 TO 1960")

    val pressureDatasetDf4 = spark.createDataFrame(finalPressureDatasetRdd4, dfSchema4)

    val finalPressureDatasetDf4 = pressureDatasetDf4.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf4.createOrReplaceTempView(metric9(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric9(2))

    spark.sql(metric9(3))

    /*Method : 2*/

    //finalPressureDatasetDf4.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1938_1960_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }

  def processPressureDataset5 {
    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1961 TO 2012---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1961 TO 2012")

   def pressureDataset5_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset5 = scala.io.Source.fromURL(metric10(0))
        pressureDataset5
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset5 =pressureDataset5_load()
	
	val pressureDatasetRdd5 = spark.sparkContext.parallelize(pressureDataset5.mkString.split("\n"))
	
    logger.info("defining schema FOR THE RANGE 1961 TO 2012")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 1961 TO 2012")

    val finalPressureDatasetRdd5 = pressureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1961 TO 2012")

    val pressureDatasetDf5 = spark.createDataFrame(finalPressureDatasetRdd5, dfSchema5)

    val finalPressureDatasetDf5 = pressureDatasetDf5.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf5.createOrReplaceTempView(metric10(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric10(2))

    spark.sql(metric10(3))

    /*Method : 2*/

    // finalPressureDatasetDf5.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1961_2012_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }

  def processPressureDataset6 {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    def pressureDataset6_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset6 = scala.io.Source.fromURL(metric11(0))
        pressureDataset6
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset6 =pressureDataset6_load()
	
	val pressureDatasetRdd6 = spark.sparkContext.parallelize(pressureDataset6.mkString.split("\n"))
	
    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val finalPressureDatasetRdd6 = pressureDatasetRdd6.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val pressureDatasetDf6 = spark.createDataFrame(finalPressureDatasetRdd6, dfSchema6)

    val finalPressureDatasetDf6 = pressureDatasetDf6.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf6.createOrReplaceTempView(metric11(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric11(2))

    spark.sql(metric11(3))

    /*Method : 2*/

    //finalPressureDatasetDf6.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_2013_2017_manual_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

  }
  def processPressureDataset7 {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    def pressureDataset7_load(): scala.io.BufferedSource = {
      try {
        val pressureDataset7 = scala.io.Source.fromURL(metric12(0))
        pressureDataset7
      } catch {
        case ex: Throwable =>
          logger.info("Error while downloading the dataset from the provided link ")
          throw new Exception
      }
    }
    
   
    val pressureDataset7 =pressureDataset7_load()
	
	val pressureDatasetRdd7 = spark.sparkContext.parallelize(pressureDataset7.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

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

    logger.info("creating final data with data type conversion FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val finalPressureDatasetRdd7 = pressureDatasetRdd7.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val pressureDatasetDf7 = spark.createDataFrame(finalPressureDatasetRdd7, dfSchema7)

    val finalPressureDatasetDf7 = pressureDatasetDf7.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf7.createOrReplaceTempView(metric12(1))

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql(metric12(2))

    spark.sql(metric12(3))

    /*Method : 2*/

    // finalPressureDatasetDf7.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_2013_2017_auto_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/
  }
  /*--------Checking data---------*/

  spark.table("default.pressure_obs_1756_1858_parq").show(10, false)
  spark.table("default.pressure_obs_1859_1861_parq").show(10, false)
  spark.table("default.pressure_obs_1862_1937_parq").show(10, false)
  spark.table("default.pressure_obs_1938_1960_parq").show(10, false)
  spark.table("default.pressure_obs_1961_2012_parq").show(10, false)
  spark.table("default.pressure_obs_2013_2017_manual_parq").show(10, false)
  spark.table("default.pressure_obs_2013_2017_auto_parq").show(10, false)

  /*--------------------------------*/

}