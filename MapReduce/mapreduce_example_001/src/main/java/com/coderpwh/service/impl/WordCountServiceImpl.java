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
            String[] args = new String[10];
            args[0]="wordCount";
            args[1]="wordCount";
            args[2]="wordCount";
            args[3]="wordCount";

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.31.101:9000");
            String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("Usage: wordcount <in> [<in>...] <out>");
                System.exit(2);
            }

            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCountServiceImpl.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            for (int i = 0; i < otherArgs.length - 1; i++) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "success";
    }


}
