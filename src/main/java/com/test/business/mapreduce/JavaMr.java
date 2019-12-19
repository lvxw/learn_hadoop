package com.test.business.mapreduce;

import com.test.common.JavaMrBaseProgram;

import java.util.Map;

/**
    -DuserMrClass=com.test.business.mapreduce.JavaMr
    -Dyarn.resourcemanager.address=master:8032
    -Dyarn.resourcemanager.scheduler.address=master:8030
    -Dyarn.resourcemanager.resource-tracker.address=master:8031
    -Dyarn.resourcemanager.admin.address=master:8033
    -Dmapreduce.framework.name=yarn
    -Dmapred.jar=jar/LearnHadoop.jar
    -Dmapreduce.app-submission.cross-platform=true
    -Dfs.defaultFS=hdfs://master:9000
    -Dinput_dir=/tmp/test/input
    -Doutput_dir=/tmp/test/output
    -DhadoopUser=root
 */

/**
    -DuserMrClass=com.test.business.mapreduce.JavaMr
    -Dinput_dir=data/input
    -Doutput_dir=data/output
 */

/**
 -DuserMrClass=com.test.business.mapreduce.JavaMr
 -Dfs.defaultFS=alluxio://master:19998/
 -Dinput_dir=/alluxio_hbase_test.txt
 -Doutput_dir=/output
 -DhadoopUser=root
 -Dalluxio.security.login.username=root
 */
public class JavaMr extends JavaMrBaseProgram {
    @Override
    protected void initComponentClassMap(Map<String, Class<?>> componentClassMap) {
        componentClassMap.put("mapper", RunWcJob.WcMapper.class);
        componentClassMap.put("reducer", RunWcJob.WcReducer.class);
        componentClassMap.put("combiner", RunWcJob.WcReducer.class);
    }

}
