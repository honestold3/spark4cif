package com.finupgroup.cif.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by wq on 2017/2/28.
 */
public class FileNameInputFormat extends FileInputFormat<Text, Text>{

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) throws IOException, InterruptedException {

        FileNameRecordReader fn = new FileNameRecordReader();


        fn.initialize(split, context);


        return fn;
    }
}
