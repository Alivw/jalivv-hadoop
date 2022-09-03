package cn.jalivv.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
