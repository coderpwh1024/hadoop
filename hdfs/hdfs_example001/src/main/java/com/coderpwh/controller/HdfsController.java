package com.coderpwh.controller;

import com.coderpwh.service.FileService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coderpwh
 */
@RequestMapping("/hdfs")
@RestController
public class HdfsController {


    @Resource
    private FileService fileService;


    @RequestMapping(value = "/mergeFile", method = RequestMethod.GET)
    public String mergeFile() {
        return fileService.mergeFile();
    }

}
