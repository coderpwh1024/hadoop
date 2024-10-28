package com.coderpwh.controller;

import com.coderpwh.service.HbaseService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coderpwh
 */
@RequestMapping("/hbase")
@RestController
public class HbaseController {

    @Resource
    private HbaseService  hbaseService;

}
