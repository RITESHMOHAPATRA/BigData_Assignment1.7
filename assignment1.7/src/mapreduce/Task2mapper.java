package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task2mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		//Checking if company name or product name must not equal to NA
		if(!(lineArray[0].equals("NA")||lineArray[1].equals("NA")))
		{
			context.write(new Text(lineArray[0]), new LongWritable(1));
		}
		
	}
}
