package org.apache.giraph.examples;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SSSPVertexInputFormat extends 
	TextVertexValueInputFormat<LongWritable, DoubleWritable, FloatWritable> {

	/** Separator of the vertex and value */
	  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	  
	@Override	
	public TextVertexValueReader createVertexValueReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongDoubleNullVertexValueReader();
	}
	
	  /**
	   * Vertex reader associated with
	   * {@link org.apache.giraph.io.formats.SSSPVertexValueInputFormat}.
	   */
	  
	public class LongDoubleNullVertexValueReader extends
    TextVertexValueReaderFromEachLineProcessed<VertexWithValue> {

		@Override
		protected VertexWithValue preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
		    return new VertexWithValue(Long.valueOf(tokens[0]),
		              Double.valueOf(tokens[1]));
		}

		@Override
		protected LongWritable getId(VertexWithValue line) throws IOException {
			return new LongWritable(line.getSrc());
		}

		@Override
		protected DoubleWritable getValue(VertexWithValue line)
				throws IOException {
			return new DoubleWritable(line.getValue());
		}
		
	   }
	
	  public class VertexWithValue {
		  private long src;
		  private double value;
		  
		  public VertexWithValue(long id, double val) {
			  this.src = id;
			  this.value = val;
			  }
		  
		  public void setSrc(long id) {
			  this.src = id;
		  }
		  		  
		  public void setValue(double val) {
			  this.value = val;
		  }
		  public long getSrc() {
			  return this.src;
		  }
		  
		  public double getValue() {
			  return this.value;
		  }
		  
	  }
}
