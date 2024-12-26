package com.coderpwh.service.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import com.coderpwh.service.WordCountService;
import com.coderpwh.service.mapreduce.IntSumReducer;
import com.coderpwh.service.mapreduce.TokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.springframework.stereotype.Service;

/**
 * @author coderpwh
 */
@Service
public class WordCountServiceImpl implements WordCountService {


    /***
     *
     * @return
     */
    @Override
    public String getWordCountInfo() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.31.101:9000");
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCountServiceImpl.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("hdfs://192.168.31.101:9000/user/hadoop/b"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.31.101:9000/user/hadoop/a"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "success";
    }


}
