/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.numberofearthquakes_magtype;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class Number_MagType_Reducer extends Reducer <Text, IntWritable, Text, IntWritable>{
    
    private IntWritable result = new IntWritable();

        public void reduce(Text key,Iterable <IntWritable> values,Context context) throws IOException,InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum = sum + val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
}
