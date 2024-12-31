package com.coderpwh.common.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author coderpwh
 */
@Configuration
public class SparkConfig {


    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName("SparkSpringBootApp")
                // 设置远程 Spark Master URL
                .setMaster("spark://localhost:7077")
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "2g");
    }

    @Bean
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    public SparkSession sparkSession(JavaSparkContext javaSparkContext) {
        return SparkSession.builder()
                .appName("SparkSpringBootApp")
                .config(javaSparkContext.getConf())
                .getOrCreate();
    }


}
