package spark.app.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  Runner.run(conf, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)
    counts.saveAsTextFile(outputFile)
  }
}

object WordCount {
  /**
    * A slightly more complex than normal wordcount example with optional
    * separators and stopWords. Splits on the provided separators, removes
    * the stopwords, and converts everything to lower case.
    */
  def withStopWordsFiltered(rdd : RDD[String],
    separators : Array[Char] = " ".toCharArray,
    stopWords : Set[String] = Set("the")): RDD[(String, Int)] = {

    val tokens: RDD[String] = rdd.flatMap(_.split(separators).
      map(_.trim.toLowerCase))
    val lcStopWords = stopWords.map(_.trim.toLowerCase)
    val words = tokens.filter(token =>
      !lcStopWords.contains(token) && (token.length > 0))
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }
}
