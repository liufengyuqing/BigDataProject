package com.imooc.hadoop.mapreducejoin.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * create by liuzhiwei on 2020/4/24
 */
public class JoinWritable implements Writable {
    // 标签，用于区分data来自于用户表还是订单表
    private String tag;
    // 数据
    private String data;

    public JoinWritable() {
    }

    public JoinWritable(String tag, String data) {
        this.tag = tag;
        this.data = data;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "JoinWritable{" +
                "tag='" + tag + '\'' +
                ", data='" + data + '\'' +
                '}';
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(tag);
        out.writeUTF(data);
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readUTF();
        data = in.readUTF();
    }
}
