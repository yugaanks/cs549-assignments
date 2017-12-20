package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node+rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */
		String node=sections[0].split("\\+")[0];
		String rs=sections[0].split("\\+")[1];
		Double rank=Double.parseDouble(rs);
		String[] adjacent_list=sections[1].split(" ");
		int i=0;
		while(i<adjacent_list.length) {
			context.write(new Text(adjacent_list[i]), new Text(String.valueOf(rank/adjacent_list.length)));
			i++;
		}
		context.write(new Text(node), new Text("-"+sections[1]));

	}

}
