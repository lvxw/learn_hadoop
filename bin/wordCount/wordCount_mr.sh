#!/bin/bash
###############################################################################
#Script:        wordCount_mr.sh
#Author:        吕学文<2622478542@qq.com>
#Date:          2019-03-27
#Description:
#Usage:         wordCount_mr.sh
#Jira:
###############################################################################

#设置脚本运行环境和全局变量
function set_env(){
    cd `cd $(dirname $0)/../.. && pwd`
    source bin/common/init_context_env.sh day $1
}

#设置日、周、月的数据输入、输出路径
function init(){
    hdfs_input_dir=/tmp/wordCount
    hdfs_output_dir=/tmp/output
}

function execute_mr(){
    hadoop jar jar/Artemis.jar com.test.WordCountMr \
        -Dinput_dir=${hdfs_input_dir} \
        -Doutput_dir=${hdfs_output_dir}
}

set_env $1
init
execute_mr