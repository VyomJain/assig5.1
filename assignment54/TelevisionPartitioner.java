package assignment54;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class TelevisionPartitioner extends Partitioner<CPSKey, Text> {

	final String[] reducer ={"Samsung","Onida","Akai","Lava","Zen"};
		
	@Override
	public int getPartition(CPSKey key, Text value, int arg2) {
		// TODO Auto-generated method stub
		String companyName = key.getCompanyName();
		
		if(companyName.equals(reducer[0]))
			return 0;
		else if(companyName.equals(reducer[1]))
			return 1;
		else if(companyName.equals(reducer[2]))
			return 2;
		else if(companyName.equals(reducer[3]))
			return 3;
		else
			return 4;
	}

}
