/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the pressure observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1861),(1862-1937),
 (1938-1960),(1961-2012),(2013-2017,manual station),(2013-2017,automatic station)}----*/
/*---------------------------------------------------------------------------------------------*/

package com.data.observation.load

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.log4j.Logger
import com.typesafe.config._
import java.io.File
import scala.collection.mutable.ArrayBuffer
import com.data.observation.load.schema._

class pressureLoadExecutor(var spark: SparkSession) extends dataFrameSchema {
  val logger = Logger.getLogger(this.getClass.getName)

  /*-----------------Below snippet is written to pass the 4 values for each dataset from the temperaturePressure.properties file
1.link for the different datasets
2.final temporary table name from which data is loaded to final table
3.create statement of the final table
4.overwrite query for the final table(fields are separated by \n)
--------------------*/

  val cfg = ConfigFactory.parseFile(new File("C:/cfg_files/temperaturePressure.properties"))
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

  /*----------------------generic function for loading pressure datasets-------------------------------*/

  def pressureDataset_load(i: String): scala.io.BufferedSource = {
     val pressureDataset = scala.io.Source.fromURL(i)
      pressureDataset
    }

  /*---------------------------------------------------------------------------------------------------*/
  
  val sch = new dataFrameSchema //  for getting the schema details from the class dataFrameSchema

  def processPressureDataset1 {

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1756 TO 1858")

    val pressureDataset1 = pressureDataset_load(metric6(0))

    val pressureDatasetRdd1 = spark.sparkContext.parallelize(pressureDataset1.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1756 TO 1858")

    val dfSchema1 = sch.pressureSchema1

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

    val pressureDataset2 = pressureDataset_load(metric7(0))

    val pressureDatasetRdd2 = spark.sparkContext.parallelize(pressureDataset2.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1859 TO 1861")

    val dfSchema2 = sch.pressureSchema2

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

    val pressureDataset3 = pressureDataset_load(metric8(0))

    val pressureDatasetRdd3 = spark.sparkContext.parallelize(pressureDataset3.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1862 TO 1937")

    val dfSchema3 = sch.pressureSchema3

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

    val pressureDataset4 = pressureDataset_load(metric9(0))

    val pressureDatasetRdd4 = spark.sparkContext.parallelize(pressureDataset4.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1938 TO 1960")

    val dfSchema4 = sch.pressureSchema4

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

    val pressureDataset5 = pressureDataset_load(metric10(0))

    val pressureDatasetRdd5 = spark.sparkContext.parallelize(pressureDataset5.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 1961 TO 2012")

    val dfSchema5 = sch.pressureSchema5

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

    val pressureDataset6 = pressureDataset_load(metric11(0))

    val pressureDatasetRdd6 = spark.sparkContext.parallelize(pressureDataset6.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val dfSchema6 = sch.pressureSchema6

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

    val pressureDataset7 = pressureDataset_load(metric12(0))

    val pressureDatasetRdd7 = spark.sparkContext.parallelize(pressureDataset7.mkString.split("\n"))

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val dfSchema7 = sch.pressureSchema7

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
  

}