package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/*
		 * TODO: emit key:node+rank, value: adjacency list Use PageRank algorithm to
		 * compute rank from weights contributed by incoming edges. Remember that one of
		 * the values will be marked as the adjacency list for the node.
		 */
		double i = 0;
		String adjacent_list = "";
		for (Text val : values) {
			if (val.toString().charAt(0) == '-') {
				adjacent_list = val.toString();
			} else {
				i = i + Double.parseDouble(val.toString());
			}
		}
		i = d * (i - 1) + 1;
		context.write(new Text(key.toString() + "+" + Double.toString(i)), new Text(adjacent_list));
	}
}
