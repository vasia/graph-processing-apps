package org.apache.giraph.examples;

import java.io.IOException;
import java.util.HashMap;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class SimpleCommunityDetection extends 
	BasicComputation<LongWritable, LongWritable, DoubleWritable, LabelWithScoreWritable> {
	
	public static final int MAX_SUPERSTEPS = 30;

	private long label;
	private double score;
	private HashMap<Long, Double> receivedLabelsWithScores = new HashMap<Long, Double>();
	private HashMap<Long, Double> labelsWithHighestScore = new HashMap<Long, Double>();

	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, DoubleWritable> vertex,
			Iterable<LabelWithScoreWritable> messages) throws IOException {
		
		// init labels and their scores
		if (getSuperstep() == 0) {
			score = 1.0;
			label = vertex.getId().get();
			// send out initial messages
			for (Edge<LongWritable, DoubleWritable> e: vertex.getEdges()){
				LongWritable neighbor = e.getTargetVertexId();
				sendMessage(neighbor, new LabelWithScoreWritable(label, 
						score * vertex.getEdgeValue(neighbor).get()));
			}
		}
		// receive labels from neighbors
		// compute the new score for each label
		// choose the label with the highest score as the new label
		// and re-score the newly chosen label
		else if (getSuperstep() < MAX_SUPERSTEPS) {
			label = vertex.getValue().get();
			
			for (LabelWithScoreWritable m: messages) {
				long receivedLabel = m.getLabel();
				double receivedScore = m.getScore();
				
				// the label has been received before
				if (receivedLabelsWithScores.containsKey(receivedLabel)) {
					double newScore = receivedScore + receivedLabelsWithScores.get(receivedLabel);
					receivedLabelsWithScores.put(receivedLabel, newScore);
				}
				// first time we receive this label
				else {
					receivedLabelsWithScores.put(receivedLabel, receivedScore);
				}
				// store label with the highest score
				if (labelsWithHighestScore.containsKey(receivedLabel)) {
					double currentScore = labelsWithHighestScore.get(receivedLabel);
					if (currentScore < receivedScore) {
						// record the highest score
						labelsWithHighestScore.put(receivedLabel, receivedScore);
					}
				}
				else {
					// first time we see this label
					labelsWithHighestScore.put(receivedLabel, receivedScore);
				}
			}
			// find the label with the highest score from the ones received
			double maxScore = -Double.MAX_VALUE;
			long maxScoreLabel = label;
			for (long curLabel : receivedLabelsWithScores.keySet()) {
				if (receivedLabelsWithScores.get(curLabel) > maxScore) {
					maxScore = receivedLabelsWithScores.get(curLabel);
					maxScoreLabel = curLabel;
				}
			}
			// find the highest score of maxScoreLabel in labelsWithHighestScore
			// the edge data set always contains self-edges for every node
			double highestScore = labelsWithHighestScore.get(maxScoreLabel);
			// re-score the new label
			if (maxScoreLabel != label) {
				// delta = 0.5
				highestScore -= 0.5f / (double) getSuperstep();
			}
			// else delta = 0
			// update own label
			vertex.setValue(new LongWritable(maxScoreLabel));

			// send out messages
			for (Edge<LongWritable, DoubleWritable> e: vertex.getEdges()){
				LongWritable neighbor = e.getTargetVertexId();
				sendMessage(neighbor, new LabelWithScoreWritable(maxScoreLabel, highestScore));
			}
		}
		else {
			vertex.voteToHalt();
		}
	}
	
}


