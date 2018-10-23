package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeywordToTweets {
	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: KeywordToTweets <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(edu.uprm.cse.bigdata.p1exam1.KeywordToTweets.class);
        job.setJobName("Find tweets that contain: Flu, Zika, Diarrhea, Ebola, Swamp, Change");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(edu.uprm.cse.bigdata.p1exam1.KeywordToTweetsMapper.class);
        job.setReducerClass(edu.uprm.cse.bigdata.p1exam1.KeywordToTweetsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

