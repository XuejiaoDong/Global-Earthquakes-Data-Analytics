package com.mycompany.avg_mag_year;

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
public class Avg_Mag_Year_Reducer extends Reducer <Text, FloatWritable, Text,FloatWritable>{
    

    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
    // called once for each key
      float sum = 0;
      float count = 0;
      float result =0;
      for(FloatWritable val : values)
      { sum = sum + val.get();
          count = count+1;
          result =sum/count;
          }

      context.write(key, new FloatWritable(result));//return key,reduceVal
    }
}
