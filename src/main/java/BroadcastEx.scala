/**
 * Created by kalit_000 on 18/12/2015.
 */

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import scala.io.Source

object BroadcastEx {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("BroadcastExample").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    // web server log file(s)
    val logfile="C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\weblogs\\*.log"

    // Read in list of target models from a local file
    val targetfile = "C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\targetmodels.txt"
    val targetlist = Source.fromFile(targetfile).getLines.toList

    // broadcast the target list to all workers
    val targetlistbc = sc.broadcast(targetlist)

    /*if we need to use braodcast variable instead of join convert braod case to hash map and use ex:- targetlistbc.value(x._1)*/


    // filter out requests from devices not in the target list
    val targetreqs = sc.textFile(logfile).filter(line => targetlistbc.value.count(line.contains(_))  > 0)

    targetreqs.take(5).foreach(println)




  }

}
