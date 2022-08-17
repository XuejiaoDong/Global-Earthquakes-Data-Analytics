package com.mycompany.max_mag_magtype;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mywatercolor
 */
public class Max_Mag_MagType_Reducer extends Reducer <Text, FloatWritable, Text,FloatWritable>{
    

    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
    // called once for each key
      float maxValue = Float.MIN_VALUE;
      for(FloatWritable val : values)
        maxValue= Math.max(maxValue,val.get());
      context.write(key, new FloatWritable(maxValue));//return key,reduceVal
    }
}
