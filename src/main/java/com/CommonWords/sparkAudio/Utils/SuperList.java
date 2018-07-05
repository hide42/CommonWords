package com.CommonWords.sparkAudio.Utils;

import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Component
public class SuperList implements Serializable {
    //atomic etc
    private volatile List<Tuple2<String,Integer>> list;

    public SuperList() {
        this.list = Collections.synchronizedList(new ArrayList<>());
    }
    public void add(Tuple2<String,Integer> tuple){
        //inserting sort = concurrent problems
        list.add(tuple);
    }
    public List<Tuple2<String,Integer>> getList(){
        //needed return copy of list or sublist
        return list;
    }
}
