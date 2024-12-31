package com.coderpwh;

import com.coderpwh.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author coderpwh
 */
@SpringBootApplication
public class SparkApplication implements CommandLineRunner {


    @Autowired
    private SparkService sparkService;

    public static void main(String[] args) {
        SpringApplication.run(SparkApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        sparkService.runSparkJob();
    }

}
