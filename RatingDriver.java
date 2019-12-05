import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class RatingDriver {
	
	
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {

            System.err.println("Less arguments provided");
            System.exit(-1);
        }
        
        /////////////////////////////////////////////////////////////////////
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "#"); 
        Job job = new Job(conf);
        job.setJarByClass(RatingDriver.class);
        job.setJobName("MR1");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, NameMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.waitForCompletion(true);
        ////////////////////////////////////////////////////////////////////
        
        Configuration conf2 = new Configuration();
        conf2.set("mapred.textoutputformat.separator", "#"); 
        Job job1 = new Job(conf2);
        job1.setJobName("MR2");
        job1.setJarByClass(RatingDriver.class);
        job1.setMapperClass(Mapper2.class);
        job1.setReducerClass(Reducer2.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        
        job1.waitForCompletion(true);
       
       //////////////////////////////////////////////////////////////////////
        
        Configuration conf3 = new Configuration();
        conf3.set("mapred.textoutputformat.separator", "#"); 
        Job job2 = new Job(conf3);
        job2.setJobName("MR3");
        job2.setJarByClass(RatingDriver.class);
        job2.setMapperClass(Mapper3.class);
        job2.setReducerClass(Reducer3.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
        
        
    }
}