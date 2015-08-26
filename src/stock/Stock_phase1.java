package stock;

import java.io.IOException;
//import java.nio.file.Path;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Stock_phase1 {


		public static class Map1 extends Mapper <Object, Text, Text, Text> {
			private Text key1 = new Text();//Set the filename as the key
			private Text value1 = new Text(); //value read from line
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String line = value.toString();				
				FileSplit fileSplit = (FileSplit)context.getInputSplit();
				  String filename = fileSplit.getPath().getName().toString();
				
				String [] data = null;
				data = line.split(",");
				Calendar cal = Calendar.getInstance();
				String date= null;
				date=data[0];
				String date_data[] = date.split("-");
				String key_to_be_set="";
				NumberFormat nf = NumberFormat.getInstance();
				if(!(data[0].contains("Date")))
				{
				//Month date_data[1] Year date_data[0]					
				key_to_be_set=filename+"-"+date_data[0]+"-"+date_data[1];	
				key1.set(key_to_be_set);
				value1.set(date_data[2]+"-"+data[6]);				
				context.write(key1, value1);				
				}
				//System.out.println("Key is "+data[0]);
				//String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				}
			}
		

		
		public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
			private Text key2 = new Text();
			private Text value2 = new Text();
			public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
				//System.out.println("Key is "+key);
				String op=null;
				ArrayList<String> day = new ArrayList<String>();
				HashMap<String, String> day_value = new HashMap<String, String>();
				String test=null;
				String data[] = null;
				String filename = null;
				String  low = null,high = null;
				int count=0;
				for (Text text : values) {
					//System.out.println("Values are "+text.toString());
					
					test=text.toString();
					data=test.split("-");
					if(count==0)
					{
						low=data[0];
						high=data[0];
						count=1;
					}
					day.add(data[0]); //data[0] is month data[1] is price
					
					if(low.compareTo(data[0])>0)
					{
						low=data[0];
					}
					if(high.compareTo(data[0])<0)
					{
						high = data[0];
					}
					day_value.put(data[0], data[1]);					
				}
				
				//System.out.println("High "+high+" Low"+low);
				double beg_price,last_price,stock_volatile_price;
				//Collections.sort(day);
				
				beg_price=Double.parseDouble(day_value.get(low));				
				last_price= Double.parseDouble(day_value.get(high));
				stock_volatile_price = (last_price-beg_price)/(beg_price);
				data = key.toString().split("-"); 
				String set_key=null,set_value=null;
				set_key =data[0];
				String volatile_price = Double.toString(stock_volatile_price);
				set_value=volatile_price;
				value2.set(set_value);
				key2.set(set_key);
				context.write(key2, value2); 	
			}
		}
	}


