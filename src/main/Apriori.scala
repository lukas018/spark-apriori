package default

import scala.reflect.ClassTag
import scala.collection.Map
import scala.collection.Set
import scala.collection.SortedSet

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD

object Apriori {

    // Run Apriori-algorithm
    def run[T: Ordering: ClassTag](
            sc: SparkContext, 
            baskets: RDD[SortedSet[T]], 
            threshold: Int, 
            size: Int
            ) :Map[SortedSet[T], Int] = {
    
        // Compute singleton counts
        println("Computing itemsets of size 1")
        val singletonCount: Map[SortedSet[T], Int] = 
            baskets.flatMap(_.subsets(1))
            .map((_, 1)).reduceByKey(_ + _)
            .filter {case(set, count) => count >= threshold}
            .collectAsMap

        val singletons: Seq[T] =  singletonCount.keySet.map {_.head}.toSeq 

        // Add singletons and broadcast result
        var collectedResults:Broadcast[Map[Int, Map[SortedSet[T], Int]]]  
                = sc.broadcast(Map[Int, Map[SortedSet[T], Int]]() + (1 -> singletonCount))
        
        var nSet: Set[SortedSet[T]] = collectedResults.value.get(1).get.keySet

        // Start iterating
        var continue = true;
        var i = 2
        while(i <= size  && continue) {
            println(s"Computing itemsets of size $i")
            // Generate candidate-sets from the baskets is faster for smaller i:s 
            var set: Map[SortedSet[T], Int] = if (i < 3)
                freqNSetBasket(i, baskets, Some(nSet), threshold)
            else
                freqNSet(sc, i, baskets, singletons, nSet, threshold)
            
            // No more supported sets exists
            continue = set.size != 0
            collectedResults = sc.broadcast(collectedResults.value + (i -> set))
            nSet = collectedResults.value.get(i).get.keySet
            i += 1
        }
        return collectedResults.value.values.reduce(_ ++ _)
    }

    // Create n-set using the baskets as source.
    // Useful for low n-values and when the baskets 
    // are not too big
    def freqNSetBasket[T: Ordering](
        size: Int, 
        baskets: RDD[SortedSet[T]], 
        freqPrevSets: Option[Set[SortedSet[T]]],
        s : Int
    ) : Map[SortedSet[T], Int] = {
        val possibleSets = baskets.flatMap { basket => 
            // Generate possible subsets of current size
            val subsets = basket.subsets(size)
            // Check if subsets are among the previosuly supported subsets
            freqPrevSets match {
                case Some(prevSets) => {
                    subsets.filter(s => s.subsets(size-1).forall(prevSets.contains(_)))
                }
                case None => subsets
            }
        }
        // Remove subsets which are below the threshold
        possibleSets.map(x => (x,1)).reduceByKey(_ + _)
        .filter {case(set, count) => count >= s}
        .collectAsMap()
    }


    // Performs the same tasks as freqNSet but uses the previous n-1 sized sets
    // to create permutations
    def freqNSet[T: Ordering] (
        sc: SparkContext,
        size: Int,
        baskets: RDD[SortedSet[T]],
        singletons: Seq[T],
        freqPrevSets: Set[SortedSet[T]],
        s: Int
    ) : Map[SortedSet[T], Int] = {
        
        // Create possible n-sized candidate sets and broadcast them to all nodes
        val possibleNextSet: Broadcast[Seq[SortedSet[T]]] = 
        sc.broadcast(
            singletons.flatMap {elem  =>  
            freqPrevSets.filter(set => !set.contains(elem))
                        .filter{set => 
                            set.subsets(size - 2).map(_ + elem)
                            .forall(freqPrevSets.contains(_))
                        }
                        .map(_ + elem)
            }.distinct
        )
        
        // Count the occurance of the n-sized sets among the baskets and filter
        baskets.flatMap{basket => 
                possibleNextSet.value.filter(_.subsetOf(basket)).map((_, 1))
        }.reduceByKey(_ + _)
        .filter {case(set, count) => count >= s}
        .collectAsMap()
        
    }

}