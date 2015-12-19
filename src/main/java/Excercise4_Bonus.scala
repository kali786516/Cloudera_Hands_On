/**
 * Created by kalit_000 on 17/12/2015.
 */
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import scala.xml._

object Excercise4_Bonus {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Excercise4_Bonus").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val devicefile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\devicestatus.txt")

    // Filter out lines with < 10 characters, use the 20th character as the delimiter, parse the line, and filter out bad lines
    val cleanstatus = devicefile.
      filter(line => line.length>20).
      map(line => line.split(line.charAt(19))).
      filter(values => values.length == 14)

    // Create a new RDD containing date, manufacturer, device ID, latitude and longitude
    val devicedata = cleanstatus.
      map(values => (values(0), values(1).split(' ')(0), values(2), values(12), values(13)))

    devicedata.take(5).foreach(println)

    devicedata.saveAsTextFile("C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\kmeans_source_file")

  }

}
