package com.test.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public abstract class JavaMrBaseProgram extends Configured implements Tool {
    private static Map<String, Class<?>> componentClassMap = new HashMap<>(4);
    protected static Configuration conf = null;
    protected static Job job = null;

    protected abstract void initComponentClassMap(Map<String, Class<?>> componentClassMap);

    protected void  prepare() throws IOException {
        initComponentClassMap(componentClassMap);

        if(conf.get("hadoopUser") != null){
            System.setProperty("HADOOP_USER_NAME", conf.get("hadoopUser"));
        }

        FileSystem.get(conf).delete(new Path(conf.get("output_dir")), true);
    }

    protected void createJob() throws IOException {
        job= Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
    }

    protected void setComponent(){
        if(componentClassMap.get("mapper") ==null){
            throw new RuntimeException("mapper is not exists");
        }else{
            job.setMapperClass((Class<? extends Mapper>) componentClassMap.get("mapper"));
        }

        if(componentClassMap.get("reducer") ==null){
            job.setNumReduceTasks(0);
        }else{
            job.setReducerClass((Class<? extends Reducer>) componentClassMap.get("reducer"));
        }

        if(componentClassMap.get("combiner") !=null){
            job.setReducerClass((Class<? extends Reducer>) componentClassMap.get("combiner"));
        }
    }

    protected  void setMrInputAndOutputPath() throws IOException {
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));
    }

    protected void setComponentGenericity() throws ClassNotFoundException {
        Type mapperType = componentClassMap.get("mapper").getGenericSuperclass();
        if(mapperType instanceof ParameterizedType)
        {
            Type[] p =((ParameterizedType) mapperType).getActualTypeArguments();
            job.setMapOutputKeyClass(Class.forName(p[2].getTypeName()));
            job.setMapOutputValueClass(Class.forName(p[3].getTypeName()));
        }

        Type reducerType = componentClassMap.get("reducer").getGenericSuperclass();
        if(reducerType instanceof ParameterizedType)
        {
            Type[] p =((ParameterizedType) reducerType).getActualTypeArguments();
            job.setOutputKeyClass(Class.forName(p[2].getTypeName()));
            job.setOutputValueClass(Class.forName(p[3].getTypeName()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        prepare();
        createJob();
        setComponent();
        setMrInputAndOutputPath();
        setComponentGenericity();

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception{
        GenericOptionsParser optionsParser = new GenericOptionsParser(new Configuration(), args);
        conf = optionsParser.getConfiguration();

        String userMrClassStr = conf.get("userMrClass");
        if(userMrClassStr == null || userMrClassStr.trim().isEmpty()){
            throw new RuntimeException("the userMrClass class is not exists");
        }else{
            int status = ToolRunner.run(new Configuration(), (Tool)Class.forName(userMrClassStr).newInstance(), args);
            System.exit(status);
        }
    }
}
