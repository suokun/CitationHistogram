import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CitationHistogram extends Configured implements Tool{
  /**
	 * @author suokun
	 * MapClass's input:
	 * CiteId,   cited numbers
	 * 1         2
	 * 10000     1
	 * 100000    1
	 * 1000006   1
	 * ...       ...
	 * 
	 * MapClass's output:
	 * cited numbers      frequency of occurrence
	 * 2                  1
	 * 1                  1
	 * 1                  1
	 * 1                  1
	 * ...                ...
	 */
	
	public static class MapClass extends MapReduceBase implements Mapper<Text, Text, IntWritable, IntWritable> {
		private final static IntWritable uno = new IntWritable(1);
		private IntWritable citationCount = new IntWritable();
		
		@Override
		public void map(Text key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			citationCount.set(Integer.parseInt(value.toString()));
			output.collect(citationCount, uno);
		}
	}
	
	
	/**
	 * @author suokun
	 * 
	 * ReduceClass's input:
	 * cited numbers      frequency of occurrence
	 * 2                  1
	 * 1                  1
	 * 1                  1
	 * 1                  1
	 * ...                ...
	 * 
	 * ReduceClass's output:
	 * cited numbers      frequency of occurrence
	 * 2                  1
	 * 1                  3
	 * ...                ...
	 */
	
	public static class ReduceClass extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while(values.hasNext()){
				count = count + values.next().get();
			}
			output.collect(key, new IntWritable(count));
		}
	}
	

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, CitationHistogram.class);

		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setJobName("CitationHistogram");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CitationHistogram(), args);
		System.exit(res);
	}
}
