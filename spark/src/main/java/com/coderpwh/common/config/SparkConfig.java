package com.coderpwh.common.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author coderpwh
 */
@Configuration
public class SparkConfig {


    @Bean
    public JavaSparkContext javaSparkContext() {
        SparkConf conf = new SparkConf()
                .setAppName("SpringBootSparkApp")
                // 根据需要设置Master URL，例如 "spark://HOST:PORT"
                .setMaster("spark://192.168.31.229")
                // 可选配置
                .set("spark.executor.memory", "1g")
                // 可选配置
                .set("spark.driver.memory", "1g");

        return new JavaSparkContext(conf);
    }

}
