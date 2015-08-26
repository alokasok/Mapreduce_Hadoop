package stock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Stock_phase3 {

		public static class Map3 extends Mapper<Object, Text, Text, Text> {			 
			private Text key1 = new Text();//set as the row and column of C
			private Text value1 = new Text(); //set as the baby value(incomplete value) of C
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String line = value.toString(); //receive one line
				String element[] = null;
				element = line.split("[\t]");								
				key1=new Text("key");
				value1 = new Text(line.toString());
			context.write(key1, value1);
				
			}
	}

		public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
			private Text key2 = new Text();
			private Text value2 = new Text();
			public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			

				TreeMap<Double,String> tm = new TreeMap<Double, String>();
				String comp_name=null;
				Double vol=(double) 0;
				
				for (Text value:values){
					//System.out.println("Value is"+value);
					String line = value.toString(); //receive one line
					String element[] = null;
					element = line.split("[\t]");
					//Company name(value) vol(key)
					comp_name=element[0].toString();
					vol=Double.parseDouble(element[1].toString());				
					tm.put(vol, comp_name);									
				}
				
				  int count=0;
				  // Get a set of the entries
			      Set<Entry<Double, String>> set = tm.entrySet();
			      // Get an iterator
			      Iterator<Entry<Double, String>> i = set.iterator();
			      // Display elements
			      key2.set("Lowest ten stocks are");
			      value2.set("");			      
			      context.write(key2, value2);
			      while(i.hasNext()&&(count<10)) {			    	 
			         Map.Entry me = (Map.Entry)i.next();
			         //System.out.print(me.getKey() + ": ");
			         //System.out.println(me.getValue());
			         key2.set(me.getValue().toString());			       
			        count++;
			        context.write(key2, value2);
			      }
			      key2.set("Top ten stocks are");
			      value2.set("");
			      context.write(key2, value2);
			      double key_data;
			      String last_val;
			      while(count>0)
			      {
			    	  key_data=tm.lastKey();
			    	  last_val=tm.get(key_data).toString();
			    	  key2.set(last_val.toString());
			    	  value2.set("");
			    	  tm.remove(key_data);
			    	  context.write(key2, value2);
			    	  count--;
			      }
				
			}
		}
}
