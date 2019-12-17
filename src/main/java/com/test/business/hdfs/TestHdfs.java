package com.test.business.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class TestHdfs {
    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.23.16.2:9000");
        FileSystem fileSystem = FileSystem.newInstance(URI.create("hdfs://172.23.16.2:9000"), conf, "root");

        boolean re1 = fileSystem.exists(new Path("/tmp/test/input"));
        System.out.println(re1);


        fileSystem.copyFromLocalFile(new Path("data/input/2.txt"), new Path("/tmp/test/input/"));
        boolean re2 = fileSystem.exists(new Path("/tmp/test/input/2.txt"));
        System.out.println(re2);

        fileSystem.copyToLocalFile(false,new Path("/tmp/test/input/1"), new Path("data/input/1.txt"), true);
        FSDataInputStream open = fileSystem.open(new Path("/tmp/test/input/2.txt"));


  /*      // 将字节输入流转化成字符输入流，并设置编码格式，InputStreamReader为 Reader 的子类
        InputStreamReader isr = new InputStreamReader(open, "UTF-8");

        // 使用 BufferedReader 进行读取
        BufferedReader bufferedReader = new BufferedReader(isr);

        String str;
        while ((str = bufferedReader.readLine()) != null){
            System.out.println(str);
        }*/

        System.out.println("----------------------------------------------------");
        IOUtils.copyBytes(open, new FileOutputStream("data/input/3.txt"), conf, true);
    }
}
