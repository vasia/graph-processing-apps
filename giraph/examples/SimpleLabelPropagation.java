package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

@Algorithm(
    name = "Simple Label Propagation"
)
public class SimpleLabelPropagation extends BasicComputation<LongWritable,
    LongWritable, NullWritable, LongWritable> {

  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 3;
  /** Number of labels **/
  public static final int MAX_LABELS = 5;
 

  @Override
  public void compute(
      Vertex<LongWritable, LongWritable, NullWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {
	  
	  HashMap<LongWritable, Integer> labelsWithFrequencies = new HashMap<LongWritable, Integer>();
	  
    if (getSuperstep() == 0) {
    	// initialize vertices with randomly picked labels
    	 Random randomGenerator = new Random();
    	vertex.setValue(new LongWritable(randomGenerator.nextInt(MAX_LABELS)));
    	// send message to all neighbors
   		sendMessageToAllEdges(vertex, vertex.getValue());
    }
    else if (getSuperstep() < MAX_SUPERSTEPS){
    	// receive labels form neighbors
    	for (LongWritable m: messages) {
    		if (!(labelsWithFrequencies.containsKey(m))) {
    			// first time we see this label
    			labelsWithFrequencies.put(m, new Integer(1));
    		}
    		else {
    			// we have seen this label before -- increase frequency
    			int freq = labelsWithFrequencies.get(m).intValue();
    			labelsWithFrequencies.put(m, new Integer(freq + 1));
    		}
    	}
    	// adopt the most frequent label
    	int maxFreq = 0;
    	long mostFrequentLabel = vertex.getValue().get();
    	
    	for (LongWritable label: labelsWithFrequencies.keySet()) {
    		if (labelsWithFrequencies.get(label) > maxFreq) {
    			maxFreq = labelsWithFrequencies.get(label);
    			mostFrequentLabel = label.get();
    		}
    	}
    	LongWritable newLabel = new LongWritable(mostFrequentLabel);
    	// send label to all neighbors
    	sendMessageToAllEdges(vertex, newLabel);
    	
    	// if value hasn't changed, vote to halt
    	if (vertex.getValue().get() != newLabel.get()) {
    		vertex.setValue(newLabel);
    	}
    	else {
        	vertex.voteToHalt();
    	}
    }
    else {
    	vertex.voteToHalt();
    }
    
  }
}
