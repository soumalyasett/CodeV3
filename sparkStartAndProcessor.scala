/*---------------------------------------------------------------------------------------------*/
/*----The below code will create the spark session and invoke the functions for loading 
 the temperature and pressure observation data into different tables----*/
/*---------------------------------------------------------------------------------------------*/

package com.implement.spark.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object sparkStartAndProcessor {
  val logger = Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]) {

    logger.info("creating spark session")

    val spark = SparkSession.builder()
      .master("local")
      .appName("Atmosphere Temperature Findings")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    val startTime = System.currentTimeMillis()
    logger.info("temperature observation load start")

    val temperatureLoad = new temperatureLoadExecutor(spark)
    temperatureLoad.processTemperatureDataset1 /*This function will load the temperature dataset for the range 1756 TO 1858 in the table default.temp_obs_1756_1858_parq*/
    temperatureLoad.processTemperatureDataset2/*This function will load the temperature dataset for the range  1859 TO 1960 in the table default.temp_obs_1859_1960_parq*/
    temperatureLoad.processTemperatureDataset3/*This function will load the temperature dataset for the range 1961 TO 2012 in the table default.temp_obs_1961_2012_parq*/
    temperatureLoad.processTemperatureDataset4/*This function will load the temperature dataset for the range 2013 TO 2017 FROM MANUAL STATION in the table default.temp_obs_2013_2017_manual_parq*/
    temperatureLoad.processTemperatureDataset5/*This function will load the temperature dataset for the range 2013 TO 2017 FROM AUTOMATIC STATION in the table default.temp_obs_2013_2017_auto_parq*/

    logger.info("temperature observation load end")

    logger.info("pressure observation load start")

    val pressureLoad = new pressureLoadExecutor(spark)
    pressureLoad.processPressureDataset1/*This function will load the pressure dataset for the range 1756 TO 1858 in the table default.pressure_obs_1756_1858_parq*/
    pressureLoad.processPressureDataset2/*This function will load the pressure dataset for the range 1859 TO 1861 in the table default.pressure_obs_1859_1861_parq*/
    pressureLoad.processPressureDataset3/*This function will load the pressure dataset for the range 1862 TO 1937 in the table default.pressure_obs_1862_1937_parq*/
    pressureLoad.processPressureDataset4/*This function will load the pressure dataset for the range 1938 TO 1960 in the table default.pressure_obs_1938_1960_parq*/
    pressureLoad.processPressureDataset5/*This function will load the pressure dataset for the range 1961 TO 2012 in the table default.pressure_obs_1961_2012_parq*/
    pressureLoad.processPressureDataset6/*This function will load the pressure dataset for the range 2013 TO 2017 FROM MANUAL STATION in the table default.pressure_obs_2013_2017_manual_parq*/
    pressureLoad.processPressureDataset7/*This function will load the pressure dataset for the range 2013 TO 2017 FROM AUTOMATIC STATION in the table default.pressure_obs_2013_2017_auto_parq*/

    logger.info("pressure observation load end")

    val endTime = System.currentTimeMillis()

    println("Total Time taken for the job: " + (endTime - startTime) / (1000) + " seconds")
    logger.info("Total Time taken for the job: " + (endTime - startTime) / (1000) + " seconds")

    spark.stop()
  }

}