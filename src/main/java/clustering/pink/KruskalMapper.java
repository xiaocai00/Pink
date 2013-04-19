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

public class KruskalMapper extends Mapper<IntWritable, EdgeWritable,  EdgeWritable, IntWritable> {
  
  protected void map(IntWritable key, EdgeWritable value, Context context) throws IOException,
      InterruptedException {
    context.write(value, key);
  }
  
  public void setup(Context context) {
  }
}