package com.test.business.mapreduce;

import com.test.common.JavaMrBaseProgram;

import java.util.Map;

/**
    -DuserMrClass=com.test.business.mapreduce.JavaMr
    -Dinput_dir=/tmp/test/input
    -Doutput_dir=/tmp/test/output
    -DdefaultFS=hdfs://172.23.16.2:9000
    -DhadoopUser=root
 */
public class JavaMr extends JavaMrBaseProgram {
    @Override
    protected void initComponentClassMap(Map<String, Class<?>> componentClassMap) {
        componentClassMap.put("mapper", RunWcJob.WcMapper.class);
        componentClassMap.put("reducer", RunWcJob.WcReducer.class);
        componentClassMap.put("combiner", RunWcJob.WcReducer.class);
    }

}
