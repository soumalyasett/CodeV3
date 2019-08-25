/*--------------The below code is a unit test case 
 where we are verifying the count at the source and the final table populated
 for both the temperature observation data and pressure observation data 
 ---------------- */

package com.implement.spark.sparkdemo.sparkUnitTest

import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions._
import org.junit.Test

class temperaturePressureProcessorTest {
  
   val spark = SparkSession.builder()
  .master("local")
  .appName("Word Count Final")
  //.config("spark.sql.catalogImplementation","hive")
  .getOrCreate()
  
  @Test def test() {
     
      val temperatureDataset1 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
      .mkString
      .split("\n")
      
      val temperatureDataset2 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
      .mkString
      .split("\n")
      
      val temperatureDataset3 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
      .mkString
      .split("\n")
      
      val temperatureDataset4 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
      .mkString
      .split("\n")
      
      val temperatureDataset5 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
      .mkString
      .split("\n")
      
      val pressureDataset1 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1756_1858.txt")
      .mkString
      .split("\n")
      
      val pressureDataset2 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1859_1861.txt")
      .mkString
      .split("\n")
      
      val pressureDataset3 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1862_1937.txt")
      .mkString
      .split("\n")
      
      val pressureDataset4 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1938_1960.txt")
      .mkString
      .split("\n")
      
      val pressureDataset5 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1961_2012.txt")
      .mkString
      .split("\n")
      
      val pressureDataset6 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_2013_2017.txt")
      .mkString
      .split("\n")
      
      val pressureDataset7 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholmA_barometer_2013_2017.txt")
      .mkString
      .split("\n")
      
      val sourceCount1=temperatureDataset1.size
      val sourceCount2=temperatureDataset2.size
      val sourceCount3=temperatureDataset3.size
      val sourceCount4=temperatureDataset4.size
      val sourceCount5=temperatureDataset5.size
      val sourceCount6=pressureDataset1.size
      val sourceCount7=pressureDataset2.size
      val sourceCount8=pressureDataset3.size
      val sourceCount9=pressureDataset4.size
      val sourceCount10=pressureDataset5.size
      val sourceCount11=pressureDataset6.size
      val sourceCount12=pressureDataset7.size
      
      val obj = new temperaturePressureProcessor
     assert(obj.processTemperatureDataset1 == sourceCount1)
     assert(obj.processTemperatureDataset2 == sourceCount2)
     assert(obj.processTemperatureDataset3 == sourceCount3)
     assert(obj.processTemperatureDataset4 == sourceCount4)
     assert(obj.processTemperatureDataset5 == sourceCount5)
     assert(obj.processPressureDataset1 == sourceCount6)
     assert(obj.processPressureDataset2 == sourceCount7)
     assert(obj.processPressureDataset3 == sourceCount8)
     assert(obj.processPressureDataset4 == sourceCount9)
     assert(obj.processPressureDataset5 == sourceCount10)
     assert(obj.processPressureDataset6 == sourceCount11)
     assert(obj.processPressureDataset7 == sourceCount12)
      
      }

  spark.stop()

}