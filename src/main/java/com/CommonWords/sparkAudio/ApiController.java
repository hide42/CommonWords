package com.CommonWords.sparkAudio;

import com.CommonWords.sparkAudio.Utils.SuperList;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import scala.Tuple2;

import javax.annotation.PostConstruct;


@RequestMapping("api")
@Controller
public class ApiController {
    @Autowired
    Consumer consumer;

    @Autowired
    SuperList superList;

    @Autowired
    JavaSparkContext javaSparkContext;

    @PostConstruct
    public void init(){
        new Producer("localhost:9092").start();
        consumer.start();
    }

    @RequestMapping("wordcount")
    public ResponseEntity words() {
        return new ResponseEntity(javaSparkContext.parallelize(superList.getList()).mapToPair(tuple2->tuple2).reduceByKey((a, b) -> a + b)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(10),HttpStatus.OK);
    }
}
