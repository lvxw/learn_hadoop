package com.test.business.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class RunWcJob {
    static class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] words = StringUtils.split(value.toString(), ' ');
            for (String w : words) {
                context.write(new Text(w), new IntWritable(1));
            }
        }
    }

    static class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum = sum + i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        // 创建本次mr程序的job实例
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.23.16.2:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        Job job = Job.getInstance(conf);

        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path("/tmp/test/output"), true);

        // 指定本次job运行的主类
        job.setJarByClass(RunWcJob.class);

        // 指定本次job的具体mapper reducer实现类
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        // 指定本次job map阶段的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定本次job reduce阶段的输出数据类型 也就是整个mr任务的最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定本次job待处理数据的目录 和程序执行完输出结果存放的目录
        FileInputFormat.setInputPaths(job, new Path("/tmp/test/input/1"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/test/output"));

        // 提交本次job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
