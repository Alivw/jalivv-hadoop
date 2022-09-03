package cn.jalivv.hadoop.wc.mappereduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyWordCount {



    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration(true);

        Job job = Job.getInstance(conf);

        // Create a new Job
        job.setJarByClass(MyWordCount.class);
        // Specify various job-specific parameters
        job.setJobName("myjob");
//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));

        Path inFIle = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job,inFIle);
        Path outFile = new Path("/data/wc/output");

        if(outFile.getFileSystem(conf).exists(outFile)) outFile.getFileSystem(conf).delete(outFile, true);
        TextOutputFormat.setOutputPath(job, outFile);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
