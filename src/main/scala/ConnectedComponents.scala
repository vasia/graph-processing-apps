package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object ConnectedComponents {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Connected Components Application")
      val sc = new SparkContext(conf)
      
      // Create an RDD for vertices
      val nodes: RDD[(VertexId, Int)] = sc.textFile("/home/vasia/Desktop/test_data/cc.nodes")
                          .map(line => line.trim()).map(id => (id.toLong, id.toInt))
      
      // Create an RDD for edges
      val edges = sc.textFile("/home/vasia/Desktop/test_data/cc.edges")
                          .map(line => Edge((line.split("\\s+")(0)).toLong, 
                              (line.split("\\s+")(1)).toLong, None))
      val graph = Graph(nodes, edges)
      
      val cc = graph.pregel(30)(
        (id, compId, newCompId) => math.min(compId, newCompId), // Vertex Program
        triplet => {  // Send Message
          if (triplet.srcAttr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr))
          } else {
            Iterator.empty
          }
        },
        (a,b) => math.min(a,b) // Merge Message
        )
      cc.vertices.saveAsTextFile("/home/vasia/Desktop/cc.out")
      }
}