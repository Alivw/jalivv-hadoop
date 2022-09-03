package cn.jalivv.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.rmi.server.ExportException;

public class TestHdfs {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() {

        conf = new Configuration(true);

        /*
           <property>
                <name>fs.defaultFS</name>
                <value>hdfs://node01:9000</value>
           </property>
         */

        try {
//            fs = FileSystem.get(conf);
            fs = FileSystem.get(URI.create("hdfs://node01:9000"), conf, "god");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Test
    public void mkdir() {
        Path dir = new Path("/jalivv");
        try {
            if (fs.exists(dir)) {
                fs.delete(dir, true);
            }
            fs.mkdirs(dir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void upload() {
        try {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream("./data/data.txt"));
            Path path = new Path("/jalivv/out.txt");
            FSDataOutputStream out = fs.create(path);
            IOUtils.copyBytes(in, out, conf, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void blocks() throws Exception {
        Path path = new Path("/user/god/data.txt");
        FileStatus file = fs.getFileStatus(path);
        BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0, file.getLen());
        for (BlockLocation location : blockLocations) {
            System.out.println(location);
        }

        /*
            0,      1048576,node01
            1048576,840319,node01
         */

        /**
         * 计算向数据移动
         * 用户和程序读取的是文件这个级别 并不知道有块的概念
         */
        FSDataInputStream in = fs.open(path);
        in.seek(1048576);
        // 计算向数据移动后，期望的是分治，只读取自己关心（通过seek实现），同时，具备距离的概念（优先和本地的DN获取数据--框架的默认机制）
        byte[] res = new byte[20];
        in.read(res, 0, 20);
        System.out.println(new String(res));

    }

    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
