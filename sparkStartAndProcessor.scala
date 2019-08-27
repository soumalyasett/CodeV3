/*---------------------------------------------------------------------------------------------*/
/*----The below code will create the spark session and invoke the functions for loading 
 the temperature and pressure observation data into different tables----*/
/*---------------------------------------------------------------------------------------------*/

package com.data.observation.load

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object sparkStartAndProcessor {
  val logger = Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]) {
    data_load()
    def data_load() {
      try {
        logger.info("creating spark session")

        val spark = SparkSession.builder()
          .master("local") //remove this when deploying to cluster , pass from spark submit
          .appName("Atmosphere Temperature Findings")
          .config("spark.sql.catalogImplementation", "hive")   // replace the mentioned with .enableHiveSupport() 
          .getOrCreate()

        val startTime = System.currentTimeMillis()
        logger.info("temperature observation load start")

        val temperatureLoad = new temperatureLoadExecutor(spark)
            temperatureLoad.processTemperatureDataset1
            temperatureLoad.processTemperatureDataset2
            temperatureLoad.processTemperatureDataset3
            temperatureLoad.processTemperatureDataset4
            temperatureLoad.processTemperatureDataset5

        logger.info("temperature observation load end")

        logger.info("pressure observation load start")

        val pressureLoad = new pressureLoadExecutor(spark)
            pressureLoad.processPressureDataset1
            pressureLoad.processPressureDataset2
            pressureLoad.processPressureDataset3
            pressureLoad.processPressureDataset4
            pressureLoad.processPressureDataset5
            pressureLoad.processPressureDataset6
            pressureLoad.processPressureDataset7

        logger.info("pressure observation load end")

        val endTime = System.currentTimeMillis()

        println("Total Time taken for the job: " + (endTime - startTime) / (1000) + " seconds")
        logger.info("Total Time taken for the job: " + (endTime - startTime) / (1000) + " seconds")

        spark.stop()
      } catch {
        case ex: Exception =>
          logger.info("Below is the exception occured : \n" + ex)
          throw new Exception
      }
    }

  }

}