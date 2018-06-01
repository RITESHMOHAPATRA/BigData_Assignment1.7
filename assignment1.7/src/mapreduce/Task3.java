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

public class Task3 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task3");
		job.setJarByClass(Task2.class);
                
                //Key is text as it is the state name
		job.setMapOutputKeyClass(Text.class);
		//Key is LongWritable as it is the no of units  of Onida
		job.setMapOutputValueClass(LongWritable.class);

                //Key is text as it is the state name
		job.setOutputKeyClass(Text.class);
		//Key is LongWritable as it is the no of units  of Onida
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Task3mapper.class);
		job.setReducerClass(Task3reducer.class);
		 
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
