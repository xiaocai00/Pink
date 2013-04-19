/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CanopyMapper extends Mapper<IntWritable, Point,  IntWritable, Point> {
  
  protected void map(IntWritable key, Point value, Context context) throws IOException,
      InterruptedException {
    context.write(key, value);
  }
  
  public void setup(Context context) {
  }
}