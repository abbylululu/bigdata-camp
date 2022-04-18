import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class SparkDistCPOptions(maxConcurrence: Int,
                              ignoreFailures: Boolean)

object Distcp {

  def mkDir(sparkSession: SparkSession, sourcePath: Path, targetPath: Path,
               fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fs.listStatus(sourcePath)
      .foreach(currPath => {
        if (currPath.isDirectory) {
          val subPath = currPath.getPath.toString.split(sourcePath.toString)(1)
          val nextTargetPath = new Path(targetPath + subPath)
          try {
            fs.mkdirs(nextTargetPath)
          } catch {
            case ex: Exception => if (!options.ignoreFailures) throw ex else printf(ex.getMessage)
          }
          mkDir(sparkSession, currPath.getPath, nextTargetPath, fileList, options)
        } else {
          fileList.append((currPath.getPath, targetPath))
        }
      })
  }

  def copy(sparkSession: SparkSession, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCPOptions): Unit = {
    val sc = sparkSession.sparkContext
    val maxConcurrentTask = options.maxConcurrence
    val rdd = sc.makeRDD(fileList, maxConcurrentTask)

    rdd.mapPartitions(ite => {
      val hadoopConf = new Configuration()
      ite.foreach(tup => {
        try {
          FileUtil.copy(tup._1.getFileSystem(hadoopConf), tup._1, tup._2.getFileSystem(hadoopConf), tup._2, false, hadoopConf)
        } catch {
          case ex: Exception => if (!options.ignoreFailures) throw ex else printf(ex.getMessage)
        }
      })
      ite
    }).collect()
  }

  def parseOptions(args: Array[String]): SparkDistCPOptions = {
      val source = args(0)
      val target = args(1)
      var ignoreFailures = true
      var maxConcurrence = 5
      args.sliding(2, 2).toList.collect {
        case Array("-i", value: String) => ignoreFailures = value.toBoolean
        case Array("-m", value: String) => maxConcurrence = value.toInt
      }
      SparkDistCPOptions(maxConcurrence, ignoreFailures)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Distcp")
      .getOrCreate()
    val sc = spark.sparkContext

    val source = args(0).toString
    val target = args(1).toString
    val options = parseOptions(args)

    var fileList: ArrayBuffer[(Path, Path)] = new ArrayBuffer[(Path, Path)]()
    mkDir(spark, new Path(source), new Path(target), fileList, options)
    copy(spark, fileList, options)
  }
}