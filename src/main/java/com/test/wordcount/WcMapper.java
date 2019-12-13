package com.test.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        System.out.println("--->Map-->" + Thread.currentThread().getName());
        String[] words = StringUtils.split(value.toString(), ' ');
        for (String w : words) {
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
