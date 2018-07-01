package com.zhuinden.sparkexperiment;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Component
public class MainService implements Serializable{
    @Autowired
    BadWords badWords;

    public List<Tuple2<String, Integer>> getTexts(JavaRDD<String> textRdd){

        return textRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
        .filter(x->!badWords.filterWords.contains(x))
        .mapToPair(w -> new Tuple2<>(w, 1))
        .reduceByKey((a, b) -> a + b)
        .mapToPair(Tuple2::swap)
        .sortByKey(false)
        .mapToPair(Tuple2::swap)
        .take(50);//.map(Tuple2::_2) ?
    }
}
