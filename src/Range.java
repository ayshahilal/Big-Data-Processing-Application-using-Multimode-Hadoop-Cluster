import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Range {

 public static class RangeMapper extends Mapper<Object, Text, Text, IntWritable>{

 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
	 
	 String[] lines = value.toString().split(System.getProperty("line.separator"));

	 for (String line: lines) {
     	String[] tokens = line.split(",");
     	word.set(tokens[9]);
     	try {
				context.write(word, one);
			} 
     	catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
     }

 }
}

public static class RangeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		int min = Integer.MAX_VALUE, max = 0;	
		int num;
		for (IntWritable val : values) {
			num = val.get();
			if (num < min) { //Finding min value
				min = num;
			}
		
			if (num > max) { //Finding max value
				max = num;
			}
		}
		int range = max-min;

		result.set(range);
	
    	context.write(key, result);


	
	} 
	
} 

public static void main(String[] args) throws Exception {

Configuration conf = new Configuration();

Job job = Job.getInstance(conf, "min max");

job.setJarByClass(MinMax.class);
job.setMapperClass(RangeMapper.class);
job.setCombinerClass(RangeReducer.class);
job.setReducerClass(RangeReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);

}

}