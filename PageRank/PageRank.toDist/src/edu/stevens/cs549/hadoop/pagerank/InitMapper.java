package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/* 
		 * TODO: Just echo the input, since it is already in adjacency list format.
		 */
		String[] sec=line.split(":");
		context.write(new Text(sec[0]), new Text(sec[1]));

	}

}
