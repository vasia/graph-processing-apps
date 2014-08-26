package org.apache.giraph.examples;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CCEdgeInputFormat extends TextEdgeInputFormat<IntWritable, NullWritable> {
	
	 /** Splitter for endpoints */
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	  @Override
	  public EdgeReader<IntWritable, NullWritable> createEdgeReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new IntNullTextEdgeReader();
	  }

	  public class IntNullTextEdgeReader extends
	      TextEdgeReaderFromEachLineProcessed<EdgeWithoutValue> {

		@Override
		protected EdgeWithoutValue preprocessLine(Text line)
				throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new EdgeWithoutValue(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1]));
		}

		@Override
		protected IntWritable getTargetVertexId(EdgeWithoutValue line)
				throws IOException {
			return new IntWritable(line.getTrg());
		}

		@Override
		protected IntWritable getSourceVertexId(EdgeWithoutValue line)
				throws IOException {
			return new IntWritable(line.getSrc());
		}

		@Override
		protected NullWritable getValue(EdgeWithoutValue line)
				throws IOException {
			return NullWritable.get();
		}
	   
	  }
	  
	  public class EdgeWithoutValue {
		  private int src;
		  private int trg;
		  
		  public EdgeWithoutValue(int first, int second) {
			  this.src = first;
			  this.trg = second;
			  }
		  
		  public void setSrc(int first) {
			  this.src = first;
		  }
		  
		  public void setTrg(int second) {
			  this.trg = second;
		  }
		  
		  public int getSrc() {
			  return this.src;
		  }
		  public int getTrg() {
			  return this.trg;
		  }		  
		  
	  }
}
