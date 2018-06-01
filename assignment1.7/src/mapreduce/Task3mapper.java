package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task3mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		//Checking if company name is Ondia or product name must not equal to NA
		if((lineArray[0].equals("Onida")&& !lineArray[1].equals("NA")))
		{
			context.write(new Text(lineArray[3]), new LongWritable(1));
		}
		
	}
}
