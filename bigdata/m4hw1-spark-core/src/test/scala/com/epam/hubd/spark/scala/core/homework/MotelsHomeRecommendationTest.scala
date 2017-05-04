package com.epam.hubd.spark.scala.core.homework

import java.io.File

import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendationTest._
import com.epam.hubd.spark.scala.core.homework.domain.BidItem
import com.epam.hubd.spark.scala.core.util.RddComparator
import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.Assert._
import org.junit.rules.TemporaryFolder



/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest {
  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_EXCHANGE_RATES_SAMPLE = "src/test/resources/exchange_rate_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = new java.io.File("c:\\tmp")

  @Before
  def setup() = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  @Test
  def shouldReadRawBids() = {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35", "0.951"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    RDDComparisons.assertRDDEquals(expected, rawBids)
  }

  @Test
  def shouldReadRawMotels() = {
    val expected = sc.parallelize(
      Seq(("0000001","Olinda Windsor Inn"), ("0000002","Merlin Por Motel"))
    )

    val rawBids = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_SAMPLE)

    RDDComparisons.assertRDDEquals(expected, rawBids)
  }

  @Test
  def shouldCollectErroneousRecords() = {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    RDDComparisons.assertRDDEquals(expected, erroneousRecords)
  }


  @Test
  def shouldReadExchangeRates() = {
    val expected = Map(
      "11-06-05-2016" -> 0.803d,
      "11-05-08-2016" -> 0.873d,
      "10-06-11-2015" -> 0.987d,
      "15-04-08-2016" -> 0.929d
    )

    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATES_SAMPLE)

    assertEquals(expected, exchangeRates)
  }


  @Test
  def shouldGetBids() = {
    val expected = sc.parallelize(
      Seq(
        BidItem("0000002", "2016-08-04 15:00", "US", 1.923d),
        //        BidItem("0000002", "2016-08-04 15:00","MX", ""),
        BidItem("0000002", "2016-08-04 15:00", "CA", 0.883d)
      )
    )
    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATES_SAMPLE)

    val bids = MotelsHomeRecommendation.getBids(rawBids, exchangeRates)
//    showRdd(bids)
    RDDComparisons.assertRDDEquals(expected, bids)
  }

  def showRdd[T](rdd: RDD[T]): Unit = {
    println(rdd.glom().collect().map(x => x.toList).toList)
  }


//  @Test
  def shouldFilterErrorsAndCreateCorrectAggregates() = {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    //printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RDDComparisons.assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }

  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {
  private var sc: SparkContext = null

  @BeforeClass
  def beforeTests() = {
    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
  }

  @AfterClass
  def afterTests() = {
    sc.stop
  }
}
