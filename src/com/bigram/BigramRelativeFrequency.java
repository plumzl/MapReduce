package com.bigram;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.io.StringPair;

public class BigramRelativeFrequency extends Configured implements Tool {

  private static class BigramMapper extends Mapper<LongWritable, Text, StringPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static StringPair bigram = new StringPair();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String previous = "";
      StringTokenizer itr = new StringTokenizer(line);

      while (itr.hasMoreTokens()) {
        String current = itr.nextToken();

        if (previous != "") {
          bigram.set(previous, current);
          context.write(bigram, one);

          bigram.set(previous, "*");
          context.write(bigram, one);
        }

        previous = current;
      }
    }
  }

  private static class BigramCombiner extends
      Reducer<StringPair, IntWritable, StringPair, IntWritable> {
    private final static IntWritable sum = new IntWritable();

    public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
      Iterator<IntWritable> iter = values.iterator();
      int intSum = 0;
      while (iter.hasNext()) {
        intSum += iter.next().get();
      }
      sum.set(intSum);
      context.write(key, sum);
    }
  }

  private static class BigramReducer extends
      Reducer<StringPair, IntWritable, StringPair, FloatWritable> {
    private final static FloatWritable value = new FloatWritable();
    private float marginal = 0.0f;

    public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (key.right.equals("*")) {
        marginal = (float) sum;
      } else {
        value.set(sum / marginal);
        context.write(key, value);
      }
    }
  }

  private static class BigramPartitioner extends
      Partitioner<StringPair, IntWritable> {

    public int getPartition(StringPair key, IntWritable value, int numReducers) {
      return (key.left.hashCode()) % numReducers;
    }
  }

  public int run(String[] args) throws Exception {
    // Creates commandline options
    Options options = new Options();
    options.addOption(OptionBuilder.hasArg()
        .withDescription("Input path") .create("inputPath"));
    options.addOption(OptionBuilder.hasArg()
        .withDescription("Output Path") .create("outputPath"));
    options.addOption(OptionBuilder.hasArg()
        .withDescription("Number of reducers").create("numReducers"));

    // Parse commandline
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException except) {
      System.err.println("Error parsing command line: " + except.getMessage());
      return -1;
    }

    if (!cmdline.hasOption("inputPath") || !cmdline.hasOption("outputPath")) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(80);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue("inputPath");
    String outputPath = cmdline.getOptionValue("outputPath");
    int numReducers = cmdline.hasOption("numReducers") ?
      Integer.parseInt(cmdline.getOptionValue("numReducers")) : 1;

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(BigramRelativeFrequency.class.getSimpleName());
    job.setJarByClass(BigramRelativeFrequency.class);
    job.setNumReduceTasks(numReducers);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(StringPair.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(StringPair.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(BigramMapper.class);
    job.setCombinerClass(BigramCombiner.class);
    job.setReducerClass(BigramReducer.class);
    job.setPartitionerClass(BigramPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    job.waitForCompletion(true);

    return 0;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BigramRelativeFrequency(), args);
  }
}
