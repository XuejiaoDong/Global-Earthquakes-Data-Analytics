/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.numberofearthquakes_magtype;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Number_MagType_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text magtype = new Text();
  
    
    public void map(LongWritable key, Text value,org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
        String[] tokens = (value.toString()).split(",");
        magtype.set(tokens[5]);
        context.write(magtype,one ); //emit(key,val)
    }
    
}
