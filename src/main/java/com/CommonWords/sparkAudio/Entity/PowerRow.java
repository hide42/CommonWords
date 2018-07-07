package com.CommonWords.sparkAudio.Entity;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class PowerRow implements Serializable {
    private long id;
    private ByteBuffer data;

    public PowerRow() {
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    public PowerRow(long id, ByteBuffer data) {

        this.id = id;
        this.data = data;
    }

    public long getId() {

        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


}
