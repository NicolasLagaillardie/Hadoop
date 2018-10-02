import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.ArrayList;
import java.util.HashSet;

public class WordCount {

	public static class WordCountMapper extends Mapper <
	LongWritable, // keyIn
	Text, // valueIn
	Text, // keyOut
	Text // valueOut
	> {
		private Text valueOutFilename;

		protected void map(LongWritable keyIn, Text valueIn, Context context) throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			Path filePath = fileSplit.getPath();
			String fileName = filePath.getName();

			valueOutFilename = new Text(fileName);

			for (String word : StringUtils.split(valueIn.toString())) {
				context.write(new Text(word), valueOutFilename);
			}
		}
	}

	public static class WordCountReducer extends Reducer <Text, Text, Text ,Text> {

		public void reduce(Text keyInWord, Iterable<Text> valuesInFileNames, Context context) throws IOException, InterruptedException {

				HashSet<String> fileNamesUnique = new HashSet<String>();

				for (Text fileName: valuesInFileNames) {
					fileNamesUnique.add(fileName.toString());
				}

				String fileNamesOut = new String( StringUtils.join(fileNamesUnique, " / ") );

				context.write(keyInWord, new Text(fileNamesOut));
			}
	}

	public static void main(String[] args) throws Exception {
 
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word count");

		job.setJarByClass (WordCount .class);
		job.setMapperClass (WordCountMapper .class);
                job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer .class);

                job.setNumReduceTasks(3);

		job.setOutputKeyClass (Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths (job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

