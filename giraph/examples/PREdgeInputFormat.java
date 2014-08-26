package org.apache.giraph.examples;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PREdgeInputFormat extends TextEdgeInputFormat<LongWritable, FloatWritable> {
	
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	  @Override
	  public EdgeReader<LongWritable, FloatWritable> createEdgeReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new LongFloatTextEdgeReader();
	  }

	  /**
	   * {@link org.apache.giraph.io.EdgeReader} associated with
	   * {@link IntNullTextEdgeInputFormat}.
	   */
	  public class LongFloatTextEdgeReader extends
	      TextEdgeReaderFromEachLineProcessed<EdgeWithFloatValue> {

		@Override
		protected EdgeWithFloatValue preprocessLine(Text line)
				throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new EdgeWithFloatValue(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]), 
					Float.valueOf(tokens[2]));
		}

		@Override
		protected LongWritable getTargetVertexId(EdgeWithFloatValue line)
				throws IOException {
			return new LongWritable(line.getTrg());
		}

		@Override
		protected LongWritable getSourceVertexId(EdgeWithFloatValue line)
				throws IOException {
			return new LongWritable(line.getSrc());
		}

		@Override
		protected FloatWritable getValue(EdgeWithFloatValue line)
				throws IOException {
			return new FloatWritable(line.getValue());
		}
		  
	   
	  }
	  
	  public class EdgeWithFloatValue {
		  private long src;
		  private long trg;
		  private float value;
		  
		  public EdgeWithFloatValue(long first, long second, float weight) {
			  this.src = first;
			  this.trg = second;
			  this.value = weight;
			  }
		  
		  public void setSrc(long first) {
			  this.src = first;
		  }
		  
		  public void setTrg(long second) {
			  this.trg = second;
		  }
		  
		  public void setValue(float weight) {
			  this.value = weight;
		  }
		  public long getSrc() {
			  return this.src;
		  }
		  public long getTrg() {
			  return this.trg;
		  }
		  public float getValue() {
			  return this.value;
		  }
		  
		  
	  }
}
