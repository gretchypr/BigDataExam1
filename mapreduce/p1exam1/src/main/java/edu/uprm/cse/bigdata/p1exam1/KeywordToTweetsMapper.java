package edu.uprm.cse.bigdata.p1exam1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text>{
	 @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		 	// Get the received JSON
		 	JsonNode twitter_json = new ObjectMapper().readTree(value.toString());
	        String newKey = "";
	        // Get full text
	        String full_text = twitter_json.get("extended_tweet").get("full_text").textValue();
	        // Get tweet id
	        String id = twitter_json.get("id_str").textValue();
	        
        	if(full_text.contains("Flu") || full_text.contains("flu")) {
 	        	newKey = "Flu";
 	        	context.write(new Text(newKey), new Text(id));
 	        }
 	        if(full_text.contains("Zika") || full_text.contains("zika")) {
 	        	newKey = "Zika";
 	        	context.write(new Text(newKey), new Text(id));
 	        }
 	        if(full_text.contains("Diarrhea") || full_text.contains("diarrhea")) {
 	        	newKey = "Diarrhea";
 	        	context.write(new Text(newKey), new Text(id));
 	        }
 	        if(full_text.contains("Ebola") || full_text.contains("ebola")) {
 	        	newKey = "Ebola";
 	        	context.write(new Text(newKey), new Text(id));
 	        }
 	        if(full_text.contains("Swamp") || full_text.contains("swamp")) {
 	        	newKey = "Swamp";
 	        	context.write(new Text(newKey), new Text(id));
 	        }
 	        if(full_text.contains("Change") || full_text.contains("change")) {
 	        	newKey = "Change";
 	        	context.write(new Text(newKey), new Text(id));
 	        }
           

	   }

}
