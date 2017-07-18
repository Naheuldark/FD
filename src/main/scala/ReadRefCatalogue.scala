/**
  * Created by AVE on 10/07/2017.
  */

import java.util.Properties

import org.apache.commons.net.nntp.Article
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types._



object ReadRefCatalogue {

  val connectionProperties = new Properties()
  connectionProperties.put("user", "userfd")
  val url = "jdbc:postgresql://VMSMARTEXCHANGE1:5432/userfd"

  case class Ref(article: String, refCommerciale: String, st: String, debValid: String, refGMR: String, hierarchProduits: String, sourceSystem: String, commercialisation: String, remark: String, offerManagement: String, scIECreferenceCatalogue: String, optimumMedium: String, iecNEMA: String, range: String, marketingOfferManager: String, logistic: String, pimStatus: String, pimDetail: String, daShortDescription: String, daFamily: String, daSubFamily: String, daSerie: String, daGpdDescription: String)
  case class Logo(article: String, refCommerciale: String, st: String, debValid: String, refGMR: String, hierarchProduitsAdded: String, hierarchProduits: String, designationArticle: String, designationArticleBis: String)
  case class GmpPtctr(materiel: String, mcr: String, materialStatuts: String, sourceSystem: String, recipient: String, sourceSystemAdded: String, recipientAdded: String, sourceAdded: String, stratProduct: String, family: String, subFamily: String, serie: String, gdpCode: String, gdpDescription: String, shortDescription: String)
  case class Source(source: String, sourceRenamed: String)
  case class Pm(level: String, node: String, gdp: String, helios: String, libFr: String, linEn: String, nodeParent: String, sType: String, codeCos: String, libCos: String, productManager: String, productManager2: String, seq: String, levelMinusOne: String, levelZero: String, levelOne: String, levelTwo: String, levelThree: String, levelFour: String, levelFive: String, levelSix: String, levelSeven: String, levelEight: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Tuto Spark Scala Official").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .appName("FD data")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .getOrCreate()

    importReference(spark)
    //importLogos(spark)
    //importGmpPtctr(spark)
    //importGmrSourceSystems(spark)
    //importpm(spark)

    spark.stop()
  }

  private def importReference(spark: SparkSession): Unit = {
    import spark.implicits._

    val refDF = spark.sparkContext
      .textFile("C:\\Users\\ave\\IdeaProjects\\FD\\data\\SC_reference_catalogue.csv")
      .map(_.split(";"))
      .map(attributes => Ref(attributes(0),attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6), attributes(7), attributes(8), attributes(9), attributes(10), attributes(11), attributes(12), attributes(13), attributes(14), attributes(15), attributes(16), attributes(17), attributes(18), attributes(19), attributes(20), attributes(21), attributes(22)))
      .toDF()

    val firstRow = refDF.first()
    val ref = refDF.filter(row => row != firstRow)

    /*
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://VMSMARTEXCHANGE1:5432/userfd")
      .option("dbtable", "alicetest.utilisateurs")
      .option("user", "userfd")
      .load()

    jdbcDF.show()
*/

    ref.write.jdbc(url, "alicetest.references", connectionProperties)
  }

  private def importLogos(spark: SparkSession): Unit = {
    import spark.implicits._

    val logosDF = spark.sparkContext
      .textFile("C:\\Users\\ave\\IdeaProjects\\FD\\data\\SAP_Logos.csv")
      .map(_.split(";"))
      .map(attributes => Logo(attributes(0),attributes(1),attributes(2),attributes(3), attributes(4),attributes(5),attributes(6),attributes(7), attributes(8)))
      .toDF()

    val firstRow = logosDF.first()
    val logos = logosDF.filter(row => row != firstRow)

    logos.write.jdbc(url, "alicetest.logos", connectionProperties)
  }

  private def importGmpPtctr(spark: SparkSession): Unit = {
    import spark.implicits._

    val gmpPtctrDF = spark.sparkContext
      .textFile("C:\\Users\\ave\\IdeaProjects\\FD\\data\\GMP_PTCTR.csv")
      .map(_.split(";"))
      .map(attributes => GmpPtctr(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6), attributes(7), attributes(8), attributes(9), attributes(10), attributes(11), attributes(12), attributes(13), attributes(14)))
      .toDF()

    val firstRow = gmpPtctrDF.first()
    val gmpPtctr = gmpPtctrDF.filter(row => row != firstRow)

    gmpPtctr.write.jdbc(url, "alicetest.gmpptctr", connectionProperties)
  }

  private def importGmrSourceSystems(spark: SparkSession): Unit = {
    import spark.implicits._

    val gmrSourceDF = spark.sparkContext
      .textFile("C:\\Users\\ave\\IdeaProjects\\FD\\data\\GMR_Source_systems.csv")
      .map(_.split(";"))
      .map(attributes => Source(attributes(0),attributes(1)))
      .toDF()

    val firstRow = gmrSourceDF.first()
    val gmrSource = gmrSourceDF.filter(row => row != firstRow)

    gmrSource.show()

    gmrSource.write.jdbc(url, "alicetest.gmrsource", connectionProperties)
  }

  private def importpm(spark: SparkSession): Unit = {
    import spark.implicits._

    val pmDF = spark.sparkContext
      .textFile("C:\\Users\\ave\\IdeaProjects\\FD\\data\\pm0.csv")
      .map(_.split(";"))
      //.map(attributes => Pm(attributes(0),attributes(1),attributes(2),attributes(3),attributes(4),attributes(5),attributes(6),attributes(7),attributes(8),attributes(9),attributes(10),attributes(11),attributes(12),attributes(13),attributes(14),attributes(15),attributes(16),attributes(17),attributes(18),attributes(19),attributes(20),attributes(21),attributes(22)))
      .map(attributes => Pm(attributes(0),attributes(1), attributes(2),attributes(3),attributes(4),attributes(5),attributes(6),attributes(7),attributes(8),attributes(9),attributes(10),attributes(11),attributes(12),attributes(13),attributes(14),attributes(15),attributes(16),attributes(17),attributes(18),attributes(19),attributes(20),attributes(21),attributes(22)))
      .toDF()

    val firstRow = pmDF.first()
    val pm = pmDF.filter(row => row != firstRow)

    pm.write.jdbc(url, "alicetest.pm", connectionProperties)

  }

}