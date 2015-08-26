package stock;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Stock_phase2 {
	
	public static class Map2 extends Mapper <Object, Text, Text, Text> {
		private Text key1 = new Text();//set as the column of A or row of B.
		private Text value1 = new Text(); //set as the rest element of each line.
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String element[] = null;
			element = line.split("[\t]");
//			for (String string : element) {
//				System.out.println("Map reduce 2 "+string);
//			}
			key1.set(element[0]);
			value1.set(element[1]);			
			//System.out.println("Map reduce 2"+line);
			
		context.write(key1, value1);
		}
	}

	
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		private Text key2 = new Text();
		private Text value2 = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double sum=0;
			double count=0;
			double data=0;
			ArrayList<Double> list_val=new ArrayList<Double>();
			//ArrayList<Double> mean_diff = new ArrayList<Double>();
			for (Text value:values){
				//System.out.println("Value is"+value);
				data = Double.parseDouble(value.toString());
				list_val.add(data);
				sum+=data;
				count++;
			}
			double mean = (sum)/(count);
			double std_dev=0,inter=0;
			for (Double double1 : list_val) {
				inter=0;
				inter=double1-mean;
				inter *= inter;
				std_dev += inter; 
			}
			double volatility = (Math.sqrt((std_dev)/(list_val.size()-1)));
			String volatile_price = Double.toString(volatility);
			value2.set(volatile_price);			
			key2.set(key);
			context.write(key2, value2); 
		}
	}

}
