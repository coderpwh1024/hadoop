package com.coderpwh.controller;

import com.coderpwh.service.SparkService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coderpwh
 */
@RequestMapping("/spark")
@RestController
public class SparkController {


    @Resource
    private SparkService sparkService;


    @RequestMapping(value = "/result",method= RequestMethod.GET)
    public String getSparkResult() {
        return sparkService.getSparkResult();
    }


}
