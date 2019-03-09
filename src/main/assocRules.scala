package default

import scala.collection.{SortedSet, Map}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD

case class AssocRule[T](from: SortedSet[T], to: SortedSet[T], conf: Double)

object AssocRules {

    // Print out an association rule
    def printRule[T](rule: AssocRule[T]): Unit = {
        val from = "(" ++ rule.from.map(_.toString).reduce((a,b) => s"$a, $b") ++ ")"
        val to = "(" ++ rule.to.map(_.toString).reduce((a,b) => s"$a, $b") ++ ")"
        val conf = rule.conf
        println(f"Confidence: $conf%2.2f | $from --> $to")
    }

    // Generate associations rule from the map of frequent itemsets
    def generateRules[T](sc: SparkContext, 
                        freqItemsMap:Map[SortedSet[T], Int],
                        confThreshold: Double) : Array[AssocRule[T]] = {

        val freqItems:Broadcast[Map[SortedSet[T], Int]]  
                    = sc.broadcast(freqItemsMap)

        val sets: RDD[SortedSet[T]] = sc.parallelize(freqItems.value.keySet.toSeq.filter(_.size > 1))

        sets.flatMap(x => generateRuleFromSet(x, freqItems, confThreshold)).distinct.collect
    }

    // We bruteforce the creation of assoc rules since we can quickly discard them if they are not valid
    def generateRuleFromSet[T](set: SortedSet[T], freq: Broadcast[Map[SortedSet[T], Int]], confThreshold: Double): Array[AssocRule[T]] = {
        
        val unionSupport = freq.value.get(set).get // Get support for the union
        set.subsets()
        .filter(x=> x.size != 0 && x.size != set.size) // Remove the complete set and the empty set
        .map{x => 
            val supportX = freq.value.get(x).get
            val confidence:Double = unionSupport / supportX.toDouble
            (x,confidence)
        }
        .filter{case(x, conf) => confThreshold < conf} // Filter out based on confidence
        .map{case(x, conf) =>
            val y= set.diff(x)
            AssocRule(x,y, conf)
        }
        .toArray
    }

}
                         