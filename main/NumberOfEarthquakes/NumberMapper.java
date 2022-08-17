package com.mycompany.numberofearthquakes;
//
//import java.io.IOException;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
///**
// *
// * @author mywatercolor
// */
//public class NumberMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
//  
//    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
//        String[] tokens = (value.toString()).split(",");
//        String[] tokens2 = (tokens[0].toString()).split("-");
//        String Year = tokens2[0];
//        float depth  = Float.parseFloat((tokens[3])); 
//        context.write(new Text(Year),new FloatWritable(depth)); //emit(key,val)
//    }
//}

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumberMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();
  
    
    public void map(LongWritable key, Text value,org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
        String[] tokens = (value.toString()).split(",");
        String[] tokens2 = (tokens[0].toString()).split("-");
        year.set(tokens2[0]);
        context.write(year,one ); //emit(key,val)
    }
    
}
