import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by JBo on 07/07/2017.
  */

object TutoSparkScalaDataset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val tf = sc.textFile("D:\\FD\\spark\\spark-2.0.0-bin-hadoop2.7\\README.md")
    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
    val counts = splits.reduceByKey((x,y)=>x+y)
    splits.saveAsTextFile("D:\\FD\\spark\\spark-2.0.0-bin-hadoop2.7\\SplitOutput")
    counts.saveAsTextFile("D:\\FD\\spark\\spark-2.0.0-bin-hadoop2.7\\CountOutput")
  }
}
