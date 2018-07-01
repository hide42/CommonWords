package com.zhuinden.sparkexperiment;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Arrays;


@RequestMapping("api")
@Controller
public class ApiController {
    @Autowired
    MainService service;

    @Autowired
    JavaSparkContext javaSparkContext;


    @RequestMapping("wordcount")
    public ResponseEntity words() {

        JavaRDD<String> textRdd = javaSparkContext.textFile("list.txt")
                .map(x->x.replaceAll("\\s+|\"", " ").trim())
                .flatMap(x-> Arrays.asList(x.split(",")).iterator()).map(TextHelper::getText);
        return new ResponseEntity(service.getTexts(textRdd), HttpStatus.OK);
    }
}
