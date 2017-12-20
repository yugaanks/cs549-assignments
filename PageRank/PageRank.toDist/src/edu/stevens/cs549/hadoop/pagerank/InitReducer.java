package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		StringBuilder sb=new StringBuilder();
		for(Text t: values) {
			sb.append(t.toString()+" ");
		}
		context.write(new Text(key.toString()+"+1.0"), new Text(sb.toString()));
	}
}
