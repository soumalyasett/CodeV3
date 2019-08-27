package com.data.observation.load.schema

import org.apache.spark.sql.types._

class dataFrameSchema {
  def temperatureSchema1: StructType = {
    val dfSchema1 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true)))
    dfSchema1
  }

  def temperatureSchema2: StructType = {

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
    dfSchema2
  }

  def temperatureSchema3: StructType = {

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
    dfSchema3
  }

  def temperatureSchema4: StructType = {
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
    dfSchema4
  }

  def temperatureSchema5: StructType = {
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
    dfSchema5
  }

  def pressureSchema1: StructType = {
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
    dfSchema1
  }

  def pressureSchema2: StructType = {
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
    dfSchema2
  }

  def pressureSchema3: StructType = {
    val dfSchema3 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInMmHg", DoubleType, true),
        StructField("NoonPressureInMmHg", DoubleType, true),
        StructField("EveningPressureInMmHg", DoubleType, true)))
    dfSchema3
  }

  def pressureSchema4: StructType = {
    val dfSchema4 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))
    dfSchema4

  }

  def pressureSchema5: StructType = {
    val dfSchema5 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))
    dfSchema5

  }

  def pressureSchema6: StructType = {
    val dfSchema6 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))
    dfSchema6
  }

  def pressureSchema7: StructType = {
    val dfSchema7 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))
    dfSchema7
  }
}

