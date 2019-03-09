# Spark Implementation of the Apriori Algorithm

This reop contains a Spark Implementation for finding Association Rules and Frequent Itemsets

The code is implemented using Scala 2.11.8

# Running the code
Run it by calling sbt "run [PathToBasketFile] [SupportThreshold] [ConfidenceThreshold]"

BasketFile can any text file, where each row corresponds to a basket, consisting of a set of indexes, each corresponding to the products.