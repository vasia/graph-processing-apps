package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SSSP {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("SSSP Application")
      val sc = new SparkContext(conf)
      
      // Create an RDD for vertices
      val nodes: RDD[(VertexId, Int)] = sc.textFile("/home/vasia/Desktop/test_data/sssp.nodes")
                          .map(line => ((line.split("\\s+")(0)).toLong, (line.split("\\s+")(1)).toInt))
      
      // Create an RDD for edges
      val edges = sc.textFile("/home/vasia/Desktop/test_data/sssp.edges")
                          .map(line => Edge((line.split("\\s+")(0)).toInt, 
                              (line.split("\\s+")(1)).toInt, (line.split("\\s+")(2)).toInt))                    
                       
      // A graph with edge attributes containing distances
      val graph = Graph(nodes, edges)
      
      // Initialize the graph such that all vertices except the root have distance infinity.
      
      val sssp = graph.pregel(30)(
        (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
        triplet => {  // Send Message
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        (a,b) => math.min(a,b) // Merge Message
        )
      sssp.vertices.saveAsTextFile("/home/vasia/Desktop/sssp.out")
      }
}