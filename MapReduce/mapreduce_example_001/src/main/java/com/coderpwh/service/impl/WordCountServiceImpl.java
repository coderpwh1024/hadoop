package com.coderpwh.service.impl;

import com.coderpwh.service.WordCountService;
import com.coderpwh.service.mapreduce.IntSumReducer;
import com.coderpwh.service.mapreduce.TokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
            String[] arr = new String[10];
            Configuration conf = new Configuration();
            String[] otherArgs = (new GenericOptionsParser(conf, arr)).getRemainingArgs();
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


        } catch (Exception e) {
            e.printStackTrace();
        }


        return null;
    }


}
