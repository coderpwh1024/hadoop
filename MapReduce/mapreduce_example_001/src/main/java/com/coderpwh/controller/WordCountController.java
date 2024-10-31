package com.coderpwh.controller;

import com.coderpwh.service.WordCountService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coderpwh
 */

@RequestMapping("/word/count")
@RestController
public class WordCountController {



     @Resource
     private WordCountService wordCountService;


    @RequestMapping(value = "/info", method = RequestMethod.GET)
    public String getWordCountInfo() {
        return wordCountService.getWordCountInfo();
    }



}
