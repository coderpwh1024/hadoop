package com.coderpwh.service.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author coderpwh
 */
public class IntSumReducer extends Reducer {

    private IntWritable result = new IntWritable();



    public void reduce(Text key,Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum=0;
        IntWritable val = null;

        for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()){
            val = (IntWritable)i$.next();
        }

        this.result.set(sum);
        context.write(key, this.result);
    }


}
