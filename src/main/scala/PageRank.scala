package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PageRank {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Page Rank")
      val sc = new SparkContext(conf)
      
      // Create an RDD for vertices
      val nodes: RDD[(VertexId, Double)] = sc.textFile("/home/vasia/Desktop/test_data/pr.nodes")
                          .map(line => ((line.split("\\s+")(0)).toLong, (line.split("\\s+")(1)).toDouble))
      
      // Create an RDD for edges
      val edges = sc.textFile("/home/vasia/Desktop/test_data/pr.edges")
                          .map(line => Edge((line.split("\\s+")(0)).toLong, 
                              (line.split("\\s+")(1)).toLong, (line.split("\\s+")(2)).toLong))
                              
      val graph = Graph(nodes, edges)
      
      val pagerank = graph.pregel(0.1, 5)(
        (id, rank, newRank) => newRank, // Vertex Program
        triplet => {  // Send Message
          val partialRank: Double = 0.15 + 0.85 * triplet.srcAttr / triplet.attr
            Iterator((triplet.dstId, partialRank))
        },
        (a,b) => a+b // Merge Message
        )
      pagerank.vertices.saveAsTextFile("/home/vasia/Desktop/pr.out")
      }
}