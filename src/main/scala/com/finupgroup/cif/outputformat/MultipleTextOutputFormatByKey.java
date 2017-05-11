/**
 * 
 */
package com.finupgroup.cif.outputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * @author lenovo
 * 
 */
public class MultipleTextOutputFormatByKey extends
		MultipleTextOutputFormat<Text, Text> {

	/**
	 * Use they key as part of the path for the final output file.
	 */
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			String leaf) {
		return new Path(key.toString(), leaf).toString();
	}

	/**
	 * When actually writing the data, discard the key since it is already in
	 * the file path.
	 */
	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
	}
}
