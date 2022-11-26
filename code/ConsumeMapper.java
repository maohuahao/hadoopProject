package com.mao.consumeReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ConsumeMapper extends Mapper<LongWritable, Text, Text, ConsumeBean> {
    private Text outK = new Text();
    private ConsumeBean outV = new ConsumeBean();
    private String filename = null;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, ConsumeBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ConsumeBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        if (filename.contains("pk")){
            outK.set(split[0]);
            outV.setName(split[1]);
            outV.setConsume(0);
            outV.setFlag("pk");
        }else{
            outK.set(split[0]);
            outV.setName("");
            outV.setConsume(Integer.parseInt(split[1]));
            outV.setFlag("detial");
        }

        context.write(outK, outV);
    }
}
