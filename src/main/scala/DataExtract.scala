import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by JBo on 12/07/2017.
  */

object DataExtract {

  def saveDFtoCSV(output: String, data: DataFrame): Unit = {
    data.repartition(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("C:\\tmp\\tmpCSV.csv")

    val pattern = "^part-r-00000.*(.csv)$".r

    val tmpCSV = new File("C:\\tmp\\tmpCSV.csv")
    tmpCSV.listFiles().foreach(f => {
      pattern findFirstIn f.getName match {
        case Some(s) => f.renameTo(new File(output.toString))
        case None => f.delete()
      }
    })
    tmpCSV.delete()
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Data Extraction").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("Data Extraction")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val refDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load("D:\\FD\\SC reference catalogue 2017 V3.0.csv")
    refDF.createOrReplaceTempView("References")

    val pm0DF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load("D:\\FD\\2017_FINAL_PM0_COS_TREE - PM0 vs COS pyramides with levels by line_30112016 v3 TIH.csv")
    pm0DF.createOrReplaceTempView("PM0")

    val o2DF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load("D:\\FD\\20170711 - O2 Countries database (sales in euros).csv")
    o2DF.createOrReplaceTempView("O2")

    //refDF.printSchema()
    //pm0DF.printSchema()
    //o2DF.printSchema()

    var results = spark.sql("SELECT * FROM O2 WHERE Entity='FR'")
    results.show()

    //val selectedData = refDF.select("Ref commerciale - MCR", "PIM status", "PIM detail")

    saveDFtoCSV("D:\\FD\\O2_French_Sales_2017.csv", results)
  }

}
