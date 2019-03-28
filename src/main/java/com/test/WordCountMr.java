package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class WordCountMr extends Configured implements Tool {


    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable ONE = new LongWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordArr = value.toString().split("\\s");
            for(String word : wordArr){
                context.write( new Text(word), ONE);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private MultipleOutputs<Text,LongWritable> mos;
        private String outputBaseDir;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, LongWritable>(context);
            outputBaseDir = context.getConfiguration().get("output_dir");
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable value : values) {
                total += value.get();
            }
            mos.write(key,new LongWritable(total), outputBaseDir + File.separator + key.toString() + File.separator + key.toString());
//            context.write(key, new LongWritable(total));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get()+100;
            }
            context.write(key, new LongWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser optionsParser = new GenericOptionsParser(getConf(), args);
        Configuration conf = optionsParser.getConfiguration();

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(WordCountMr.class);

        Path output = new Path(conf.get("output_dir"));
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output, true);
        }

        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        job.setNumReduceTasks(3);

        int status = job.waitForCompletion(true) ? 0 : 1;
        return status;
    }


    public static void main(String[] args) throws Exception{
        int status = ToolRunner.run(new Configuration(), new WordCountMr(), args);
        System.exit(status);
    }
}
