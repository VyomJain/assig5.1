package assignment54;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TelevisionReducer extends Reducer<CPSKey, Text, CPSKey, IntWritable> {
	
	int count;
	
	public void reduce(CPSKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		count = 0;
		
		for(Text value : values)
			count++;
		
		context.write(key, new IntWritable(count));
	}
}
