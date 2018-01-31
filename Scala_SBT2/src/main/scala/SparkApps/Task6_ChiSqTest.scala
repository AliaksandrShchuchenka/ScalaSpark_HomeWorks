package SparkApps

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

import scala.math._


// a vector composed of the frequencies of events

object Task6_ChiSqTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark CSV SQL")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()
    implicit
    // Generate a random double RDD that contains 1 million i.i.d. values drawn from the
    // standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
    val startScoreRange = 0
    val endScoreRange = 11
    val seed = 1
    val sigma = 1.5
    val mu = 5.5
    val countElements = 100000L
    val scoreType: Array[Double] = Range(startScoreRange,endScoreRange,seed).toArray.map(x => x.toDouble)

    //Calculate fact distribution frequency
    val factFreq = normalRDD(spark.sparkContext, countElements)
                    .map(x => mu + x*sigma)
                    .histogram(scoreType,true)
                      .map(x=>x.toDouble)

    //Calculate sum of values of actual distribution frequency, which are in 0-10 range
    var i=0
    var n: Double = 0
    var xm: Double = 0
    while (i < factFreq.length) {
      n += factFreq(i)
      xm += scoreType(i)*factFreq(i)
      i += 1
    }
    var selAvg = xm/n.toDouble
    //Calculate sum of values of actual distribution frequency, which are in 0-10 range
//    i=0
//    //var n: Long = 0
//    while (i < factFreq.length) {
//      n += factFreq(i)
//      i += 1
//    }

    //Calculate expected distribution frequency
    var expectedFreq: Array[Double] = new Array(endScoreRange-1)
    i=0
    //var n: Long = 0   //Calculate sum of actual distribution frequency
    while (i < expectedFreq.length) {
      var ti = (scoreType(i) + seed/2 - mu)/sigma
      var fi = exp(-1*pow(ti,2)/2)/sqrt(2*Pi)
      var ni = seed*n*fi/sigma
      //var ni =
      expectedFreq(i) = ni
      i += 1
    }

    val goodnessOfFitTestResult = Statistics.chiSqTest(Vectors.dense(factFreq),Vectors.dense(expectedFreq))
    println(goodnessOfFitTestResult)
    spark.stop()
    //println(s"$goodnessOfFitTestResult\n")
  }
}
