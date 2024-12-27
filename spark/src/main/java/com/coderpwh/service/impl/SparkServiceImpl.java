package com.coderpwh.service.impl;

import com.coderpwh.service.SparkService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
//import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import org.apache.spark.api.java.function.Function;

/**
 * @author coderpwh
 */
@Slf4j
@Service
public class SparkServiceImpl implements SparkService {



    @Resource
    private JavaSparkContext javaSparkContext;



    /***
     * spark
     * @return
     */
    @Override
    public String getSparkResult() {
        StringBuilder builder = new StringBuilder();

        String logFile = "file:///usr/local/hadoop/spark/README.md";
//        SparkConf conf = new SparkConf().setMaster("192.168.31.229").setAppName("SparkServiceImpl");

//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> logData = sc.textFile(logFile).cache();

        JavaRDD<String> logData=   javaSparkContext.textFile(logFile).cache();;
        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();
        long numBs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();


        log.info("a:{}", numAs);
        log.info("b:{}", numBs);
        builder.append("a:").append(numAs).append(" ,").append("b:").append(numBs);
        return builder.toString();
    }


}
