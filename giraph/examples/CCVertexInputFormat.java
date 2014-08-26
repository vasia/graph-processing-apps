package org.apache.giraph.examples;

import java.io.IOException;	

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CCVertexInputFormat extends 
	TextVertexValueInputFormat<IntWritable, IntWritable, NullWritable> {
	  
	@Override	
	public TextVertexValueReader createVertexValueReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new IntVertexValueReader();
	}
	
	  /**
	   * Vertex reader associated with
	   * {@link org.apache.giraph.io.formats.SSSPVertexValueInputFormat}.
	   */
	  
	public class IntVertexValueReader extends
	TextVertexValueReaderFromEachLineProcessed<Integer> {

		@Override
		protected Integer preprocessLine(Text line) throws IOException {
			return Integer.parseInt(line.toString());
		}

		@Override
		protected IntWritable getId(Integer line) throws IOException {
			return new IntWritable(line);
		}

		@Override
		protected IntWritable getValue(Integer line) throws IOException {
			return new IntWritable(line);
		}
		
	   }
}
