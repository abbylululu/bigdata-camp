import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.FileSplit
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.SparkSession

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Inverted Index")
      .getOrCreate()

    // Delete output file if exists
//    val hadoopConf = new org.apache.hadoop.conf.Configuration()
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://emr-header-1.cluster-285604:9000"), hadoopConf)
//    try {
//      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
//    } catch {
//      case _: Throwable => {}
//    }

    val tempIndex = spark.sparkContext.wholeTextFiles(args(0))
      .flatMap {
        case (path, text) =>
          text.toString.split("\r\n")
            .flatMap(line => line.split(" "))
            .map {
              word => ((word, path.split("/").last), 1)
            }
      }.reduceByKey {
        case (n1, n2) => n1 + n2
      }.map {
        case ((w, p), n) => (w, (p, n))
      }.groupBy {
        case(w, (p, n)) => w
      }.map {
        case (w, seq) => {
          val seq2 = seq map {
            case (_, (p, n)) => (p, n)
          }
          (s"$w:${seq2.mkString(",")}")
        }
      }.saveAsTextFile(args(1))

    //    val tempIndex = spark.sparkContext.wholeTextFiles(args(0))
    //      .flatMap {
    //        case (path, text) =>
    //          text.split("\n\r")
    //          .map {
    //            word => (word, path.split("/").last)
    //          }
    //      }
    //
    //    val invertedIndex = tempIndex.groupByKey()
    //
    //    val group = invertedIndex.map {
    //      case (word, tup) =>
    //        val fileCountMap = scala.collection.mutable.HashMap[String, Int]()
    //        for (fileName <- tup) {
    //          val count = fileCountMap.getOrElseUpdate(fileName, 0) + 1
    //          fileCountMap.put(fileName, count)
    //        }
    //        (word, fileCountMap)
    //    }.sortByKey().map(word => s"${word._1}:${word._2}")
    //
    //    group.repartition(1).saveAsTextFile(args(1))
  }
}