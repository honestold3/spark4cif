/**
 * 
 */
package com.finupgroup.cif.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * @author wq
 * 
 */
public class MultipleOutputFormatByKey extends
		MultipleOutputFormat<Text, Text> {

	/**
	 * Use they key as part of the path for the final output file.
	 */
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			String leaf) {
		//return new Path(key.toString(), leaf).toString();
		//return new Path(key.toString(), value.toString()+"2013").toString();
		return new Path(value.toString(), value.toString()).toString();
	}

	/**
	 * When actually writing the data, discard the key since it is already in
	 * the file path.
	 */
	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
		//return key;
	}

	@Override
	protected RecordWriter<Text, Text> getBaseRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
