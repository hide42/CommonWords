package com.CommonWords.sparkAudio;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.PostConstruct;
import java.util.List;


@RequestMapping("api")
@Controller
public class ApiController {
    private List<String> list;
    @PostConstruct
    public void init(){
        List<String> rddList = javaSparkContext.textFile("list.txt").map(x->x.replaceAll("\\s+|\"", " ").trim()).collect();
        list = DumbParallelScarp.parallelSort(rddList.toArray(new String[rddList.size()]));
    }

    @Autowired
    MainService service;

    @Autowired
    JavaSparkContext javaSparkContext;


    @RequestMapping("wordcount")
    public ResponseEntity words() {
        //Reduce by key mojno particiyami parallel'na
        //и вообще тут все убого , нужно parallelize

        return new ResponseEntity(service.getTexts(javaSparkContext.parallelize(list)), HttpStatus.OK);
    }
}
