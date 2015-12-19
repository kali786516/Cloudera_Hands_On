/**
 * Created by kalit_000 on 18/12/2015.
 */

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._


object SparkAccumlator {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SaprkAccum").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val jpgcount = sc.accumulator(0)
    val htmlcount = sc.accumulator(0)
    val csscount = sc.accumulator(0)

    val filename="C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\weblogs\\*.log"
    val logs = sc.textFile(filename)

    logs.foreach(line => {
      if (line.contains(".html")) htmlcount += 1
      else if (line.contains(".jpg")) jpgcount += 1
      else if (line.contains(".css")) csscount += 1
    })

    println("Request Totals:")
    println(".css requests: "+ csscount.value)
    println(".html requests: " + htmlcount.value)
    println(".jpg requests: " + jpgcount.value)


  }

}
