/**
 * Created by kalit_000 on 17/12/2015.
 */
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._

object Excercise5_Join {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Excercise5_join").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val weblogsfile=sc.textFile("C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\weblogs\\*6.log")

    // map each request (line) to a pair (userid, 1) then sum the hits
    val userreqs = weblogsfile.
      map(line => line.split(' ')).
      map(words => (words(2),1)).
      reduceByKey((v1,v2) => v1 + v2)

    // Step 2 - return a user count for each hit frequency
    val freqcount = userreqs.map(pair => (pair._2,pair._1)).countByKey()

    // Step 3 - Group IPs by user ID
    val userips = weblogsfile.
      map(line => line.split(' ')).
      map(words => (words(2),words(0))).
      groupByKey()
    // print out the first 10 user ids, and their IP list
    for (pair <- userips.take(10)) {
      println(pair._1 + ":")
      for (ip <- pair._2) println("\t"+ip)
    }

    // Step 4a - map account data to (userid,[values....])
    val accountsdata = "C:\\Users\\kalit_000\\Desktop\\Hadoop_IMP_DOC\\spark\\spark_labs\\training_materials\\sparkdev\\data\\accounts.csv"
    val accounts = sc.textFile(accountsdata).
      map(line => line.split(',')).
      map(account => (account(0),account))

   /* Ignore join and use braodcast example ....
    val accountsbc = sc.broadcast(accounts.collect().toMap)
    accounts.map(v => (v._1, (accountsbc.value(v._1), v._2)))
    */

    // Step 4b - Join account data with userreqs then merge hit count into valuelist
    val accounthits = accounts.join(userreqs)

    // Step 4c - Display userid, hit count, first name, last name for the first few elements
    for (pair <- accounthits.take(10)) {
      printf("%s, %s, %s, %s\n",pair._1,pair._2._2, pair._2._1(3),pair._2._1(4))
    }

    // Bonus exercises
    // key accounts by postal/zip code
    var accountsByPCode = sc.textFile(accountsdata).map(_.split(',')).keyBy(_(8))

    // map account data to lastname,firstname
    var namesByPCode = accountsByPCode.mapValues(values => values(4) + ',' + values(3)).groupByKey()

    // print the first 5 zip codes and list the names
    for (pair <- namesByPCode.sortByKey().take(5)) {
      println("---" + pair._1)
      pair._2.foreach(println)
    }

  }

}
