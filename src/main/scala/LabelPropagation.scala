package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import collection.mutable.Map
import scala.util.Random

object LabelPropagation {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Label Propagation Application")
      val sc = new SparkContext(conf)
      
      // Create an RDD for vertices
      val nodes: RDD[(VertexId, Long)] = sc.textFile("/home/vasia/Desktop/test_data/lp.nodes")
                          .map(line => line.trim()).map(id => (id.toLong, (Random.nextInt(5)).toLong))
      
      // Create an RDD for edges
      val edges = sc.textFile("/home/vasia/Desktop/test_data/lp.edges")
                          .map(line => Edge((line.split("\\s+")(0)).toLong, 
                              (line.split("\\s+")(1)).toLong, None))
      val graph = Graph(nodes, edges)
      
      // TODO: add score in the vertex value and compare with own score
      // before updating label? or always have a self-edge
      val lp = graph.pregel[Map[Long, Long]](Map(0L -> 0), 10)(  
        (id, label, labelsWithCnts) => findMaxScoreLabel(labelsWithCnts), // Vertex Program
        triplet => {  // Send Message
            Iterator((triplet.dstId, Map(triplet.srcId -> 1L)))
        },
        (a,b) => updateLabelsMap(a, b) // Merge Message
        )
      lp.vertices.saveAsTextFile("/home/vasia/Desktop/lp.out")
      }

  def findMaxScoreLabel(labelsWithCnts: Map[Long, Long]): Long = {
      var maxScore = -100L
      var maxScoreLabel = -1L
      labelsWithCnts.keys.foreach{ i =>
        if (labelsWithCnts(i) > maxScore) {
          maxScoreLabel = i
          maxScore = labelsWithCnts(i)
        }
       }
      return maxScoreLabel
    }

  def updateLabelsMap(a: Map[Long,Long], b: Map[Long,Long]): Map[Long, Long] = {
      b.keys.foreach{ k =>
        if (a.contains(k)) {
          // the label is already present in a -> increase score
          var currentScore = a.remove(k).get
          a.put(k, currentScore + 1)
        }
        else {
          a.put(k, b.get(k).get)
        }
      }
      return a
    }
}