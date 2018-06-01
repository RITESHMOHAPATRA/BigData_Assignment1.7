package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3reducer extends Reducer<Text, LongWritable, Text, LongWritable>
{	
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
	{
	        //Summing the values no of units for a particular key which is the state name.
		long totalSales = 0;
		for(LongWritable value:values)
		{
			totalSales+= value.get(); 
		}
		context.write(key,new LongWritable(totalSales));
	}
}

