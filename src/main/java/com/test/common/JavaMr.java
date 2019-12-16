package com.test.common;

import com.test.wordcount.WcMapper;
import com.test.wordcount.WcReducer;

import java.util.Map;

/**
    -DuserMrClass=com.test.common.JavaMr
    -Dinput_dir=/tmp/test/input
    -Doutput_dir=/tmp/test/output
    -DdefaultFS=hdfs://172.23.16.2:9000
    -DhadoopUser=root
 */
public class JavaMr extends JavaMrBaseProgram {
    @Override
    protected void initComponentClassMap(Map<String, Class<?>> componentClassMap) {
        componentClassMap.put("mapper", WcMapper.class);
        componentClassMap.put("reducer", WcReducer.class);
        componentClassMap.put("combiner", WcReducer.class);
    }

}
