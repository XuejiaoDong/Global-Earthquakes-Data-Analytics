package com.mycompany.avg_mag_magtype;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author mywatercolor
 */
public class Avg_Mag_MagType_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
    
    //private final static IntWritable one = new IntWritable(1);
   // private Text stocks = new Text();
  
    
    public void map(LongWritable key, Text value,Mapper.Context context) throws IOException, InterruptedException{
        String[] tokens = (value.toString()).split(",");
        String magType = tokens[5];
        float mag  = Float.parseFloat((tokens[4])); 
        context.write(new Text(magType),new FloatWritable(mag)); //emit(key,val)
    }
}
