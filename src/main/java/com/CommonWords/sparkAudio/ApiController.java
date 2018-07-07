package com.CommonWords.sparkAudio;

import com.CommonWords.sparkAudio.Entity.SuperMap;
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
    SuperMap superMap;

    @Autowired
    JavaSparkContext javaSparkContext;

    @PostConstruct
    public void init(){
        new Producer("localhost:9092").start();
        consumer.start();
    }

    @RequestMapping("wordcount")
    public ResponseEntity words() {
        return new ResponseEntity(javaSparkContext.parallelize(superMap.getList()).mapToPair(tuple2->tuple2)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2->Tuple2._2+" "+Integer.toString(Tuple2._1))//mapToPair(Tuple2::swap) для визуализации мб
                .take(30),HttpStatus.OK);
    }
}
