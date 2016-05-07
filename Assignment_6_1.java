import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Assignment_6_1{
	public static class Mapperr extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields =line.split("	");
			if(fields[5].equals("Swimming")){
				context.write(new Text(fields[2]),new IntWritable(new Integer(Integer.parseInt(fields[9]))));
			}
		}
	}
	public static class Reducerr extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val: values){
				sum+=val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=new Job(conf,"ACADGILD Assignment_6_1");
		job.setJarByClass(Assignment_6_1.class);
		job.setMapperClass(Assignment_6_1.Mapperr.class);
		job.setReducerClass(Assignment_6_1.Reducerr.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));// "olympix_data.csv"
		FileOutputFormat.setOutputPath(job,new Path(args[1]));// "asm.txt"
		job.waitForCompletion(true);
	}
}