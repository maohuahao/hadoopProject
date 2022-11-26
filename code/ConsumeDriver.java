package com.mao.consumeReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ConsumeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ConsumeDriver.class);
        job.setMapperClass(ConsumeMapper.class);
        job.setReducerClass(ConsumeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ConsumeBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ConsumeBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
