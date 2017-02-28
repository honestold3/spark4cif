package com.finupgroup.cif.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Created by wq on 2017/2/28.
 */
public class FileNameRecordReader extends RecordReader<Text, Text> {


    String fileName;

    LineRecordReader lr =new LineRecordReader();


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {


        lr.initialize(split, context);


        //获取当前InputSplit的文件名
        fileName=((FileSplit)split).getPath().getName();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        //return new Text("("+fileName+"@"+ lr.getCurrentKey().toString()+")");
        return new Text(fileName);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return lr.getCurrentValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        return lr.nextKeyValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return lr.getProgress();
    }

    @Override
    public void close() throws IOException {

        lr.close();
    }
}
