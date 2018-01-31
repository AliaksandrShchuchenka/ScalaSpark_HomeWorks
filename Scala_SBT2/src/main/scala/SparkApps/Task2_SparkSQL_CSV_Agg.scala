package SparkApps

import org.apache.spark.sql.SparkSession
object Task2_SparkSQL_CSV_Agg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark CSV SQL")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    val ds = spark.read.format("csv").option("header", "true").load("Salaries_Of_Employees.csv")

    ds.createOrReplaceTempView("employees")

    val result = spark.sqlContext.sql("" +
      "select count(*) as count_employees" +
      ", avg(salary) as avg_salary" +
      ", sum(salary) as sum_salary" +
      ", max(salary) as max_salary " +
      "from employees")

    println(result.show())

    spark.stop()
  }
}
