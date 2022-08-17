package com.mycompany.avg_mag_year;

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
public class Avg_Mag_Year_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
    
    //private final static IntWritable one = new IntWritable(1);
   // private Text stocks = new Text();
  
    
    public void map(LongWritable key, Text value,Mapper.Context context) throws IOException, InterruptedException{
        String[] tokens = (value.toString()).split(",");
        String[] tokens2 = (tokens[0].toString()).split("-");
        String Year = tokens2[0];
        float mag  = Float.parseFloat((tokens[4])); 
        context.write(new Text(Year),new FloatWritable(mag)); //emit(key,val)
    }
}
