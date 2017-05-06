package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, udf, row_number}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.BigDecimal.RoundingMode.HALF_UP
import scala.util.Try

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }


  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    sqlContext.read.parquet(bidsPath)
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    import rawBids.sqlContext.implicits._
    rawBids
      .filter($"HU".like("%ERROR%"))
      .withColumn("ErrCount", lit(1))
      .withColumnRenamed("HU", "ErrorName")
      .groupBy($"BidDate", $"ErrorName").sum("ErrCount")
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "false") //reading the headers
      .schema(Constants.EXCHANGE_RATES_HEADER)
      .load(exchangeRatesPath)
      .select("ValidFrom", "ExchangeRate")
  }

  def getConvertDate: UserDefinedFunction = udf((str: String) => {
    Constants.INPUT_DATE_FORMAT.parseDateTime(str).toString(Constants.OUTPUT_DATE_FORMAT)
  })

  def tryDouble(str: String): Option[Double] = Try(str.toDouble).toOption

  def roundTo3Digits(dbl: Double): Double = BigDecimal(dbl).setScale(3, HALF_UP).toDouble

  def calcPriceInEuro: UserDefinedFunction = udf((first: String, second: String) => {
    roundTo3Digits(tryDouble(first).getOrElse[Double](0) * tryDouble(second).getOrElse[Double](-1))
  })

  case class ExplodedData(loSa: String, price: String)

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    import rawBids.sqlContext.implicits._
    val baseCols = Seq("MotelID", "BidDate")
    val targetColumnNames = baseCols ++ Constants.TARGET_LOSAS
    val requiredCols = targetColumnNames.map(colName => $"$colName")
    val targetCols = (baseCols ++ Seq("LoSa", "EuroPrice")).map(col => $"$col")

    def rowCol(name: String) = targetColumnNames.indexOf(name)

    rawBids
      .filter(!$"HU".like("%ERROR%"))
      .explode(requiredCols: _*) { row => Constants.TARGET_LOSAS.map(losa => ExplodedData(losa, row(rowCol(losa)).asInstanceOf[String])) }
      .join(exchangeRates.withColumnRenamed("ValidFrom", "BidDate"), "BidDate")
      .withColumn("EuroPrice", calcPriceInEuro($"Price", $"ExchangeRate"))
      .filter($"EuroPrice" > 0)
      .withColumn("BidDate", getConvertDate($"BidDate"))
      .select(targetCols: _*)

  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    val df = sqlContext.read.parquet(motelsPath)
    df.select(df("MotelID"), df("MotelName"))
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    import bids.sqlContext.implicits._
    val outputCols = Seq("MotelID", "MotelName", "BidDate", "LoSa", "EuroPrice").map(col => $"$col")

    val win = Window.partitionBy($"MotelID", $"BidDate").orderBy($"EuroPrice".desc)

    val frame = bids.join(motels, "MotelID")
      .withColumn("rn", row_number().over(win)).where($"rn".equalTo(1))
      .select(outputCols: _*)
    frame.show
    frame

    /*
    val frame = bids
      .as[Record]
      .groupBy($"MotelID", $"BidDate")
      .reduce((x, y) => if (x.EuroPrice > y.EuroPrice) x else y)


    frame.show
    frame.toDF()
      .join(motels, "MotelID")
      .select(outputCols: _*)
*/
  }

}

case class Record(MotelID: String, BidDate: String, LoSa: String, EuroPrice: Double)
