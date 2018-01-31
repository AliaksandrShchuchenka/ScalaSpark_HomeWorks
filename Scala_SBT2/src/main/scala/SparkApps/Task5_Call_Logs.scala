package SparkApps

import java.io.{BufferedWriter, FileWriter}
import java.sql.Timestamp
import java.time.LocalDateTime

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


// Create the case classes for our domain
case class Call(start_timest: String, from: BigInt, to: BigInt, duration: Int, region: String, position: BigInt)

object Task5_Call_Logs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark CSV SQL")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()
      implicit

    // Create the Departments
      def randomString(length: Int) = {
        val r = new scala.util.Random
        val sb = new StringBuilder
        for (i <- 1 to length) {
          sb.append(r.nextPrintableChar)
        }
        sb.toString
      }

    var callsList = new ListBuffer[Call]()
    var callsListStr = new ListBuffer[Array[String]]()
    val offset = Timestamp.valueOf("2016-01-01 00:00:00").getTime
    val end = Timestamp.valueOf("2017-01-01 00:00:00").getTime
    val diff = end - offset + 1

    val outputFile = new BufferedWriter(new FileWriter("CallLog.csv"))
    val csvFields = Array("start_timest", "from", "to", "duration", "region", "position")
    val csvWriter = new CSVWriter(outputFile)

    callsListStr += csvFields
    var i = 0
    for (i <- 1 until 1700000) {
      callsListStr += Array(
        new Timestamp(offset + (Math.random * diff).toLong).toString
        ,(20000000+Math.random*1000000).toLong.toString
        ,(20000000+Math.random*1000000).toLong.toString
        ,(Math.random*600).toInt.toString
        ,randomString(2).replace(",","A")
        ,i.toString
      )
    }

    var start = Timestamp.valueOf(LocalDateTime.now()).getTime
    csvWriter.writeAll(callsListStr.toList)
    outputFile.close()
    var finish = Timestamp.valueOf(LocalDateTime.now()).getTime
    val csv_writing_time = (finish-start).toFloat/1000

    start = Timestamp.valueOf(LocalDateTime.now()).getTime
    val dfCSV = spark.read.option("header","true").csv("CallLog.csv")
    finish = Timestamp.valueOf(LocalDateTime.now()).getTime
    val csv_reading_time = (finish-start).toFloat/1000

    start = Timestamp.valueOf(LocalDateTime.now()).getTime
    dfCSV.write.mode(SaveMode.Overwrite).parquet("CallLog_Parquet")
    finish = Timestamp.valueOf(LocalDateTime.now()).getTime
    val prq_writing_time = (finish-start).toFloat/1000

    start = Timestamp.valueOf(LocalDateTime.now()).getTime
    dfCSV.write.mode(SaveMode.Overwrite).csv("CallLog_Copy.csv")
    finish = Timestamp.valueOf(LocalDateTime.now()).getTime
    val df_CSV_writing_time = (finish-start).toFloat/1000

    start = Timestamp.valueOf(LocalDateTime.now()).getTime
    val dfPrq = spark.read.option("header","true").parquet("CallLog_Parquet")
    finish = Timestamp.valueOf(LocalDateTime.now()).getTime
    val prq_reading_time = (finish-start).toFloat/1000

    dfPrq.createOrReplaceTempView("calls")

    val result = spark.sqlContext.sql("" +
      "SELECT " +
      " `from` as who" +
      ", sum(duration) as sum_salary" +
      " FROM calls" +
      " GROUP BY `from`" +
      " ORDER BY sum(duration) DESC" +
      " LIMIT 5")
    println(result.show())

    //dfPrq.show(10)
    spark.stop()
    println("Time of writing CSV file (csvWriter): "+csv_writing_time)
    println("Time of writing CSV file (dataframe): "+df_CSV_writing_time)
    println("Time of reading CSV file (dataframe): "+csv_reading_time)
    println("Time of wring Parquet file (dataframe): "+prq_writing_time)
    println("Time of reading Parquet file (dataframe): "+prq_reading_time)
  }
}
