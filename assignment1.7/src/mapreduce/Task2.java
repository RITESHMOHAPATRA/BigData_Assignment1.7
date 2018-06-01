package mapreduce;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Task2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task2");
		job.setJarByClass(Task2.class);
                
                //Key is text as it is the company_name
		job.setMapOutputKeyClass(Text.class);
		//Key is LongWritable as it is the no of units
		job.setMapOutputValueClass(LongWritable.class);
                
                //Key is text as it is the company_name
		job.setOutputKeyClass(Text.class);
		//Key is LongWritable as it is the no of units
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Task2mapper.class);
		job.setReducerClass(Task2reducer.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
