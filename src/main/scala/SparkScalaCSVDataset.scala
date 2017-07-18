import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by JBo on 07/07/2017.
  */
object TutoSparkScalaCSVDataset {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Tuto Spark Scala Official").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    //runBasicDataFrameExample(spark)
    //runDatasetCreationExample(spark)
    //runInferSchemaExample(spark)
    //runProgrammaticSchemaExample(spark)
    runDataFrameFromCSV(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("D:\\FD\\people.json")

    df.show()

    import spark.implicits._
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()

    val path = "D:\\FD\\people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleDF = spark.sparkContext
      .textFile("D:\\FD\\people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleRDD = spark.sparkContext.textFile("D:\\FD\\people.txt")

    val schemaString = "name age"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")

    var results = spark.sql("SELECT name FROM people")
    results.map(attributes => "Name: " + attributes(0)).show()
  }

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

  private def runDataFrameFromCSV(spark: SparkSession): Unit = {
    import spark.implicits._

    val csvDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load("D:\\FD\\SC reference catalogue 2017 V3.0.csv")

    //csvDF.printSchema()

    val selectedData = csvDF.select("Ref commerciale - MCR", "PIM status", "PIM detail")

    saveDFtoCSV("D:\\FD\\test_res.csv", selectedData)

    println("Done")
  }
}
