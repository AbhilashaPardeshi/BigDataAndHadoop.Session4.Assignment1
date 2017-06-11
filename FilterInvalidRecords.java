import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author abhilasha
 *
 */
public class FilterInvalidRecords {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		//Check if input parameters provided appropriately
		if(args==null || args.length!=2)
		{
			System.err.println("Incorrect input parameters provided");
			System.exit(-1);
		}
	
		
		//Instantiate configuration object
		Configuration conf = new Configuration();
		
		//Instantiate job object
		Job job  = new Job(conf,"FilterInvalidRecords");
		
	
		job.setJarByClass(FilterInvalidRecords.class);
	
		/*
		 * Set input pathput
		 * abhilasha/television.txt
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		/*
		 * Set output path
		 * eg : /abhilasha/FilterInvalidRecordsOutput
		 */
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Delete output directory if already existing
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		//Set mapper class
		job.setMapperClass(FilterInvalidRecords_Mapper.class);
		
		//Set input and output format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set output key'value class types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//The job with no reducer
		job.setNumReduceTasks(0);
			
		//Execute the job and wait until completion and then exit
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
