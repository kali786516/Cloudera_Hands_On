/**
 * Created by kalit_000 on 17/12/2015.
 */
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import scala.xml._



object Excercise4_Xml {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Excercise4").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    // Given a string containing XML, parse the string, and
    // return an iterator of activation XML records (Nodes) contained in the string
    def getactivations(xmlstring: String): Iterator[Node] = {
      val nodes = XML.loadString(xmlstring) \\ "activation"
      nodes.toIterator
    }

    // Given an activation record (XML Node), return the model name
    def getmodel(activation: Node): String = {
      (activation \ "model").text
    }

    // Given an activation record (XML Node), return the account number
    def getaccount(activation: Node): String = {
      (activation \ "account-number").text
    }

   val xmlfile=sc.wholeTextFiles("C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\activations\\*.xml")

    //xmlfile.map(x => x._1+":"+getactivations(x._2))

  val activationtrees=xmlfile.flatMap(x => getactivations(x._2))

  val model=activationtrees.map(x => getaccount(x)+"," + getmodel(x))
  model.take(2).foreach(println)

  }

}
