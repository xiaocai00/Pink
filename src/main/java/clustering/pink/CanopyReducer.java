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
import org.apache.hadoop.mapreduce.Reducer;

public class CanopyReducer extends Reducer<IntWritable, Point, IntWritable, Point> {
  protected void reduce(IntWritable key, Iterable<Point> values, Context context)
      throws IOException, InterruptedException {
    // merge the canopies
    // and write out the mulitple output
  }

  public void setup(Context context) {

  }
}

