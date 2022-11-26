package com.mao.consumeReduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ConsumeReducer extends Reducer<Text, ConsumeBean, Text, ConsumeBean> {
    private ConsumeBean outV = new ConsumeBean();

    @Override
    protected void reduce(Text key, Iterable<ConsumeBean> values, Reducer<Text, ConsumeBean, Text, ConsumeBean>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        String name = null;
        for (ConsumeBean consume : values){
            if ("detial".equals(consume.getFlag())){
                sum += consume.getConsume();
            }else{
                name = consume.getName();
            }
        }

        outV.setName(name);
        outV.setConsume(sum);

        context.write(key , outV);
    }
}
