package org.apache.giraph.examples;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LPEdgeInputFormat extends TextEdgeInputFormat<LongWritable, NullWritable> {
	
	 /** Splitter for endpoints */
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	  @Override
	  public EdgeReader<LongWritable, NullWritable> createEdgeReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new LongNullTextEdgeReader();
	  }

	  public class LongNullTextEdgeReader extends
	      TextEdgeReaderFromEachLineProcessed<EdgeWithoutValue> {

		@Override
		protected EdgeWithoutValue preprocessLine(Text line)
				throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new EdgeWithoutValue(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1]));
		}

		@Override
		protected LongWritable getTargetVertexId(EdgeWithoutValue line)
				throws IOException {
			return new LongWritable(line.getTrg());
		}

		@Override
		protected LongWritable getSourceVertexId(EdgeWithoutValue line)
				throws IOException {
			return new LongWritable(line.getSrc());
		}

		@Override
		protected NullWritable getValue(EdgeWithoutValue line)
				throws IOException {
			return NullWritable.get();
		}
	   
	  }
	  
	  public class EdgeWithoutValue {
		  private long src;
		  private long trg;
		  
		  public EdgeWithoutValue(long first, long second) {
			  this.src = first;
			  this.trg = second;
			  }
		  
		  public void setSrc(long first) {
			  this.src = first;
		  }
		  
		  public void setTrg(long second) {
			  this.trg = second;
		  }
		  
		  public long getSrc() {
			  return this.src;
		  }
		  public long getTrg() {
			  return this.trg;
		  }		  
		  
	  }
}
