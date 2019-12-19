package com.test.common

import java.io.IOException
import java.lang.reflect.ParameterizedType

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{GenericOptionsParser, Tool, ToolRunner}

import scala.collection.mutable.Map

abstract class ScalaMrBaseProgram extends Configured with Tool{
  lazy private val componentClassMap = Map[String,Class[_]]()
  protected var conf: Configuration = _
  protected var job: Job = _

  protected def initComponentClassMap(componentClassMap: Map[String, Class[_]]): Unit

  @throws[IOException]
  protected def prepare(): Unit = {
    initComponentClassMap(componentClassMap)
    if (conf.get("hadoopUser") != null) System.setProperty("HADOOP_USER_NAME", conf.get("hadoopUser"))
    FileSystem.get(conf).delete(new Path(conf.get("output_dir")), true)
  }

  @throws[IOException]
  protected def createJob(): Unit = {
    job = Job.getInstance(conf, this.getClass.getSimpleName)
    job.setJarByClass(this.getClass)
  }

  protected def setComponent(): Unit = {
    if (componentClassMap.get("mapper") == None){
      throw new RuntimeException("mapper is not exists")
    }else{
      job.setMapperClass(componentClassMap.get("mapper").get.asInstanceOf[Class[_ <: Mapper[_, _, _, _]]])
    }

    if (componentClassMap.get("reducer") == None) {
      job.setNumReduceTasks(0)
    }else{
      job.setReducerClass(componentClassMap.get("reducer").get.asInstanceOf[Class[_ <: Reducer[_, _, _, _]]])
    }

    if (componentClassMap.get("combiner") != None){
      job.setReducerClass(componentClassMap.get("combiner").get.asInstanceOf[Class[_ <: Reducer[_, _, _, _]]])
    }
  }

  @throws[IOException]
  protected def setMrInputAndOutputPath(): Unit = {
    FileInputFormat.addInputPaths(job, conf.get("input_dir"))
    FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")))
  }

  @throws[ClassNotFoundException]
  protected def setComponentGenericity(): Unit = {
    val mapperType = componentClassMap.get("mapper").get.getGenericSuperclass
    if (mapperType.isInstanceOf[ParameterizedType]) {
      val p = mapperType.asInstanceOf[ParameterizedType].getActualTypeArguments
      job.setMapOutputKeyClass(Class.forName(p(2).getTypeName))
      job.setMapOutputValueClass(Class.forName(p(3).getTypeName))
    }
    val reducerType = componentClassMap.get("reducer").get.getGenericSuperclass
    if (reducerType.isInstanceOf[ParameterizedType]) {
      val p = reducerType.asInstanceOf[ParameterizedType].getActualTypeArguments
      job.setOutputKeyClass(Class.forName(p(2).getTypeName))
      job.setOutputValueClass(Class.forName(p(3).getTypeName))
    }
  }

  override def run(strings: Array[String]): Int = {
    prepare()
    createJob()
    setComponent()
    setMrInputAndOutputPath()
    setComponentGenericity()

   if (job.waitForCompletion(true)) 0 else 1
  }

  def main(args: Array[String]): Unit = {
    val optionsParser = new GenericOptionsParser(new Configuration, args)
    conf = optionsParser.getConfiguration

    val status = ToolRunner.run(new Configuration, this.asInstanceOf[Tool], args)
    System.exit(status)
  }
}
