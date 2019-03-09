package default

import scala.collection.Map
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.collection.SortedSet

object Main {

    def main(args: Array[String]) = {
        
        val (filepath, supThreshold, confidenceThreshold) 
        = if(args.length < 3) {
            println(s"Insuffient arguments. 3 are required but recieved ${args.length}.")
            println(s"Setting defult parameters")
            ("../../data/T10I4D100K.dat", 0.01, 0.5)
        } else {
            (args(0), args(1).toDouble, args(2).toDouble)
        }
        
        val spark = SparkSession.builder().appName("Frequent Itemset")
                .config("spark.master", "local").getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("ERROR") // Stop outputing error
        val rdd = sc.textFile(filepath).map(_.split(" ").map(_.toInt))
        val baskets = rdd.map(x => SortedSet(x: _*))

        println("\nStart computing frequent itemsets")
        // Compute frequent itemsets and rules
        val nBaskets = baskets.count 
        val freqSets = Apriori.run[Int](sc, baskets, (nBaskets*supThreshold).toInt, 10)

        freqSets.keySet.filter(_.size == 3).foreach(println)

        val assocRules = AssocRules.generateRules[Int](sc, freqSets, confidenceThreshold)
        println("Finished")        
        // Print Answers
        println(f"\nAssociations Rules with support-threshold: $supThreshold")
        assocRules.sortWith((x,y) => x.conf > y.conf).foreach(AssocRules.printRule[Int])
        spark.close
    }

}