package SparkApps

import org.apache.spark.sql.SparkSession


object Task4_Orders_JDBC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark CSV SQL")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    val product = spark.sqlContext.read.format("jdbc").options(Map(
        "url" -> "jdbc:db2://dashdb-mpp-yp-dal10-36.services.dal.bluemix.net:50000/BLUDB"
      , "user" -> "XXX"
      , "password" -> "XXX"
      , "dbtable" -> "ADS_APP_DEV.TMP_PRODUCT"
      , "driver" -> "com.ibm.db2.jcc.DB2Driver")).load()
    val order_item = spark.sqlContext.read.format("jdbc").options(Map(
      "url" -> "jdbc:db2://dashdb-mpp-yp-dal10-36.services.dal.bluemix.net:50000/BLUDB"
      , "user" -> "XXX"
      , "password" -> "XXX"
      , "dbtable" -> "ADS_APP_DEV.TMP_ORDER_ITEM"
      , "driver" -> "com.ibm.db2.jcc.DB2Driver")).load()
    product.createOrReplaceTempView("product")
    order_item.createOrReplaceTempView("order_item")

    val result = spark.sqlContext.sql("" +
      "WITH CTE_REV" +
      " AS " +
      "(" +
      " SELECT " +
      " P.CATEGORY_ID" +
      " ,P.PRODUCT_NAME" +
      " ,SUM(I.COST * I.QTY) AS REVENUE " +
      " FROM order_item I " +
      " INNER JOIN product P " +
      " ON I.PRODUCT_ID = P.PRODUCT_ID " +
      " GROUP BY P.CATEGORY_ID,P.PRODUCT_NAME" +
      ")" +
      " , CTE_RESULT" +
      " AS" +
      " (" +
      " SELECT " +
      " ROW_NUMBER() OVER (PARTITION BY CATEGORY_ID ORDER BY REVENUE DESC) AS RN" +
      " ,CATEGORY_ID" +
      " ,PRODUCT_NAME" +
      " ,REVENUE " +
      " FROM CTE_REV R" +
      " ) SELECT * FROM CTE_RESULT WHERE RN < 4")

    result.show(50)
    spark.stop()
  }
}
/*
    // Obtain SQLContext
  val sqlContext = new SQLContext(sc)
  // Construct DataFrame from Employee table,
  // passing the Database URL and Driver class
  // to the sqlContext.load( ) API
  val employeeDF = sqlContext.load("jdbc", Map(
    "url" -> "jdbc:db2://localhost:50000/SAMPLE:user=db2admin;password=db2admin;",
    "driver" -> "com.ibm.db2.jcc.DB2Driver",
    "dbtable" -> "EMPLOYEE"))

  // show the DataFrame contents
  employeeDF.show();
}
*/