package com.coderpwh.service.impl;

import com.coderpwh.service.SparkService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import org.apache.spark.api.java.function.Function;

import java.util.List;

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
        SparkConf conf = new SparkConf().setMaster("192.168.31.229").setAppName("11123");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

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

    @Override
    public void runSparkJob() {
        // 创建一个简单的RDD
        JavaRDD<String> rdd = javaSparkContext.parallelize(List.of("hello", "world", "spark", "spring", "boot"));

        // 执行操作
        long count = rdd.filter(word -> word.startsWith("s")).count();
        System.out.println("Count of words starting with 's': " + count);
    }


}
