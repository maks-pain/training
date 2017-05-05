package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.Constants.EXCHANGE_RATES_HEADER
import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
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

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath).map(str => str.split(Constants.DELIMITER).toList)
  }

  def containsError(lst: List[String]) = lst.map(elm => elm.contains("ERROR")).reduce((a, b) => a || b)

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids
      .filter(containsError)
      .map(lst => BidError(lst(1), lst(2)))
      .map(v => (v, 1L)).reduceByKey(_ + _)
      .map { case (bidError, number) => s"$bidError,$number" }
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    def extractDateRate(lst: List[String]) = (lst(EXCHANGE_RATES_HEADER.indexOf("ValidFrom")), lst(EXCHANGE_RATES_HEADER.indexOf("ExchangeRate")))

    sc.textFile(exchangeRatesPath).map(str => str.split(Constants.DELIMITER).toList)
      .map(extractDateRate)
      .map { case (date, rate) => (date, BigDecimal(rate).toDouble) }
      .collectAsMap().toMap
  }


  val bidRequiredColIds: Seq[Int] = Seq(
    Seq(Constants.BIDS_HEADER.indexOf("MotelID")),
    Seq(Constants.BIDS_HEADER.indexOf("BidDate")),
    Constants.TARGET_LOSAS.map(losa => Constants.BIDS_HEADER.indexOf(losa)).seq
  ).flatten


  def extractBidData(line: List[String]): List[String] = {
    bidRequiredColIds.map(idx => line(idx)).toList
  }

  def getDoubleOption(str: String): Option[Double] = Try(str.toDouble).toOption

  def convertDate(str: String) = Constants.INPUT_DATE_FORMAT.parseDateTime(str).toString(Constants.OUTPUT_DATE_FORMAT)


  def explodeByTargetLosasToBidItems(line: List[String], exchangeRates: Map[String, Double]): List[BidItem] = {
    def calcPriceInEuro(date: String, price: Double) = exchangeRates.get(date).map(rate => price.*(rate)).getOrElse(-1d)

    def roundTo3Digits(dbl: Double) = BigDecimal(dbl).setScale(3, HALF_UP).toDouble

    Constants.TARGET_LOSAS
      .toIndexedSeq.zipWithIndex
      .map { case (losa, idx) => List(line(0), line(1), losa, line(2 + idx)) }
      .map(row => (row, getDoubleOption(row(3))))
      .filter { case (row, doubleOpt) => doubleOpt.isDefined }
      .map { case (row, doubleOpt) => (row, doubleOpt.get) }
      .map { case (row, price) => (row, calcPriceInEuro(row(1), price)) }
      .filter { case (row, price) => price > 0 }
      .map { case (row, price) => BidItem(row(0), convertDate(row(1)), row(2), roundTo3Digits(price)) }
      .toList
  }

  def not[T](predicateFn: T => Boolean): T => Boolean = (data: T) => !predicateFn(data)

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val processLines = explodeByTargetLosasToBidItems(_: List[String], exchangeRates)
    rawBids.filter(not(containsError))
      .map(extractBidData)
      .flatMap(processLines)
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath).map(str => str.split(Constants.DELIMITER).toList)
      .map(row => (row(Constants.MOTELS_HEADER.indexOf("MotelID")), row(Constants.MOTELS_HEADER.indexOf("MotelName"))))
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val maxEnrichedItem: (EnrichedItem, EnrichedItem) => EnrichedItem = {
      case (item1, item2) => if (item1.price >= item2.price) item1 else item2
    }
    bids.keyBy(bi => bi.motelId)
      .join(motels)
      .map { case (motelId, (bi, motelName)) => EnrichedItem(motelId, motelName, bi.bidDate, bi.loSa, bi.price) }
      .keyBy(ei => (ei.motelId, ei.bidDate))
      .reduceByKey(maxEnrichedItem)
      .map { case (_, ei) => ei }

  }
}
