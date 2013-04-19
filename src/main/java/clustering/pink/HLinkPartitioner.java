/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HLinkPartitioner extends Partitioner<IntWritable, Point> implements Configurable {
  @Override
  public int getPartition(IntWritable key, Point value, int num) {
    return key.get();
  }

  public Configuration getConf() {
    return null;
  }

  public void setConf(Configuration arg0) {
  }
}