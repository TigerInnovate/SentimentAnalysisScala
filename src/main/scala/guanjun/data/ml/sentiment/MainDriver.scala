package guanjun.data.ml.sentiment

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by guanwang on 11/3/14.
 */

object MainDriver {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Sentiment Analysis").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val workDir = "file:///Users/guanwang/IdeaProjects/SentimentAnalysisScala/"

    val posWords = sc.textFile(workDir + "src/main/resources/Hu_Liu_positive_word_list.txt")
    val posWordsList = sc.broadcast(posWords.collect())

    val negWords = sc.textFile(workDir + "src/main/resources/Hu_Liu_negative_word_list.txt")
    val negWordsList = sc.broadcast(negWords.collect())

    val nltkStopWords = sc.textFile(workDir + "src/main/resources/stopwords/english")
    val moreStopWds = sc.parallelize(List("cant", "didnt", "doesnt", "dont", "goes", "isnt", "hes",
      "shes", "thats", "theres", "theyre", "wont", "youll", "youre",
      "youve", "br", "ve", "re", "vs", "dick", "ginger", "hollywood",
      "jack", "jill", "john", "karloff", "kudrow", "orson", "peter", "tcm",
      "tom", "toni", "welles", "william", "wolheim", "nikita"))
    val stopWordsRDD = (nltkStopWords union moreStopWds).filter(_ != "").cache()
    val stopWordsList = sc.broadcast(stopWordsRDD.collect())

    /** experimentation: data profiling **/

    val inTrainUnsup = sc.wholeTextFiles(workDir + "src/main/resources/reviews/train/unsup")

    /** Within a transformation, any action can't be used, otherwise NullPointerException will be thrown
      * Don't make RDD[ RDD[T] ], inlined RDD makes it difficult to fully utilize standard Scala features, use Broadcast instead
      * **/

    val parsedTrainUnsup = inTrainUnsup mapValues (
      _ map {
        case c: Char if Character.isLetterOrDigit(c) => c
        case _ => ' '
      }
        split (" ")
        filter (_.trim() != "")
        filter (_.length() > 1)
        map (_.toLowerCase())
        filter (!stopWordsList.value.contains(_))
      )

    val wordFreqDist = parsedTrainUnsup flatMap {
      case (x, y) => y
    } map (w => (w, 1)) reduceByKey (_ + _)

    val posTrainUnsupItems = (posWords map ((_, -1))) join wordFreqDist mapValues { case (x, y) => y}
    val sortedPosItems = posTrainUnsupItems map (_.swap) sortByKey (false) map (_.swap) //This is not useful now...

    val negTrainUnsupItems = (negWords map ((_, -1))) join wordFreqDist mapValues { case (x, y) => y}
    val sortedNegItems = negTrainUnsupItems map (_.swap) sortByKey (false) map (_.swap) //This is not useful now...

    //Get the top 25 hot items
    //implicit val is for top(25), defining sort on the 2nd element
    implicit val pairSortByValue = new Ordering[(String, Int)] {
      override def compare(a: (String, Int), b: (String, Int)) = a._2 compare b._2
    }

    println("Top 25 positive words in unsup dataset")
    posTrainUnsupItems.top(25).foreach(println)
    println("Top 25 negative words in unsup dataset")
    negTrainUnsupItems.top(25).foreach(println)

    //
    val normalizedTrainUnsup = parsedTrainUnsup flatMap { case (f, ws) => for (w <- ws) yield (w, f)}

    //This is an implementation with JOIN operation
    //val inter = normalizedTrainUnsup join (posWords map ((_, ""))) map {case (x, y) => y match {case (a, b) => (a, x)}} groupByKey() mapValues(_.size)

    //This is an implementation with Broadcast operation
    val posItemPercentage = parsedTrainUnsup.mapValues(
      ws => ((for (w <- ws; if posWordsList.value contains (w)) yield w).length) * 1.0 / ws.length)
    val negItemPercentage = parsedTrainUnsup.mapValues(
      ws => ((for (w <- ws; if negWordsList.value contains (w)) yield w).length) * 1.0 / ws.length)

    sc.stop()
  }
}
