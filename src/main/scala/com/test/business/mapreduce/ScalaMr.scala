package com.test.business.mapreduce

import java.io.IOException

import com.test.common.ScalaMrBaseProgram
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.hadoop.util.StringUtils

import scala.collection.mutable.Map
import scala.collection.JavaConverters._

/**
  *-Dinput_dir=/tmp/test/input
  *-Doutput_dir=/tmp/test/output
  *-DdefaultFS=hdfs://172.23.16.2:9000
  *-DhadoopUser=root
  */
object ScalaMr extends ScalaMrBaseProgram {
  override protected def initComponentClassMap(componentClassMap: Map[String,Class[_]]): Unit = {
    componentClassMap += ("mapper" -> classOf[WordCountMapper])
    componentClassMap += ("reducer" -> classOf[WordCountReducer])
    componentClassMap += ("combiner" -> classOf[WordCountReducer])
  }


  class WordCountMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
    val ONE = new LongWritable(1)
    @throws[IOException]
    @throws[InterruptedException]
    protected override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
      StringUtils
        .split(value.toString, ' ')
        .foreach(el => context.write(new Text(el), ONE))
    }
  }

  class WordCountReducer extends Reducer[Text, LongWritable, Text, LongWritable] {
    @throws[IOException]
    @throws[InterruptedException]
    protected override def reduce(key: Text, values: java.lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {
      val sum = values.asScala.map(el => el.get()).sum
      context.write(key, new LongWritable(sum))
    }
  }
}
