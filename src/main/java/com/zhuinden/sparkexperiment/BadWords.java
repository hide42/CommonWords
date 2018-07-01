package com.zhuinden.sparkexperiment;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Component
public class BadWords implements Serializable {

    public List<String> filterWords;

    @Value("${badWords}")
    private void setfilterWords(String[] filterWords) {
        this.filterWords = Arrays.asList(filterWords);
    }
}
