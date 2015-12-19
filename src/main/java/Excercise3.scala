/**
 * Created by kalit_000 on 17/12/2015.
 */

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._


object Excercise3_Logfiles {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

   val conf = new SparkConf().setMaster("local[*]").setAppName("Excercise3").set("spark.hadoop.validateOutputSpecs", "false")
   val sc = new SparkContext(conf)

   val logfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\weblogs\\*.log")

   val jpgcount=logfile.filter(line => line.contains(".jpg")).count()

    println(".jpg count:-%s".format(jpgcount))

   logfile.map(x => x.split(" ")).take(5)

    val ipsaddress=logfile.map(x => x.split(" ")).map(x => (x(0),x(2)))

    ipsaddress.take(5).foreach(println)
  }
}
