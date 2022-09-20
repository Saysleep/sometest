package com.jie.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class GetHdfs {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        //这里指定使用的是HDFS文件系统
        conf.set("fs.defaultFS","hdfs://10.0.0.14:8020");
        //通过如下的方式进行客户端身份的设置
        System.setProperty("HADOOP_USER_NAME","root");
        //通过FileSystem的静态方法获取文件系统客户端对象
        FileSystem fs = FileSystem.get(conf);


    }
}
