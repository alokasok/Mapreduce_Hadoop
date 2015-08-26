package stock;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
	     Job job = Job.getInstance();
	     job.setJarByClass(Stock_phase1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(Stock_phase2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(Stock_phase3.class);

		System.out.println("\n**********Stock_Volatility-> Start**********\n");

		job.setJarByClass(Stock_phase1.class);
		job.setMapperClass(Stock_phase1.Map1.class);
		job.setReducerClass(Stock_phase1.Reduce1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
//		job.setNumReduceTasks(5);// decide how many output file
		int NOfReducer1 = Integer.valueOf(2);	
		job.setNumReduceTasks(NOfReducer1);
	
//		job.setPartitionerClass(MatrixMul_phase1.CustomPartitioner.class);

		job2.setJarByClass(Stock_phase2.class);
		job2.setMapperClass(Stock_phase2.Map2.class);
		job2.setReducerClass(Stock_phase2.Reduce2.class);
		job3.setMapperClass(Stock_phase3.Map3.class);
		job3.setReducerClass(Stock_phase3.Reduce3.class);
		
		//job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(Text.class);
//		job2.setNumReduceTasks(5);
		int NOfReducer2 = Integer.valueOf(2);
		job2.setNumReduceTasks(NOfReducer2);
		int NOfReducer3 = Integer.valueOf(2);
		job3.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path("temp-1"));
//		FileInputFormat.addInputPath(job2, new Path("temp-1"));
//		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		
		FileOutputFormat.setOutputPath(job, new Path("Inter_"));
		FileInputFormat.addInputPath(job2, new Path("Inter_"));
		FileOutputFormat.setOutputPath(job2, new Path("Output_"));
		FileInputFormat.addInputPath(job3, new Path("Output_"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		job.waitForCompletion(true);
		job2.waitForCompletion(true);

		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Stock_volatility-> End**********\n");		
//		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

