package com.coderpwh.controller;

import com.coderpwh.service.HbaseService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coderpwh
 */
@RequestMapping("/hbase")
@RestController
public class HbaseController {

    @Resource
    private HbaseService  hbaseService;


    @RequestMapping(value = "/createBase", method = RequestMethod.GET)
    public String createBase() {
        return hbaseService.createBase();
    }

}
