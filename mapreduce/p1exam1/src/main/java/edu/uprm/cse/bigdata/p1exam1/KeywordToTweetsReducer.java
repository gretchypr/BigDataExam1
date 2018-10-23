package edu.uprm.cse.bigdata.p1exam1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text>{
	 @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
	        // Counter
	        ArrayList<String> tweets = new ArrayList<String>();
	        // Count
	        for (Text value : values ){
	            tweets.add(value.toString());
	        }

	        context.write(key, new Text(tweets.toString()));
	    }

}
