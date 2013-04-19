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

public class KruskalPartitioner extends Partitioner<EdgeWritable, IntWritable> implements Configurable {
  int kWay = 2;
  @Override
  public int getPartition(EdgeWritable key, IntWritable value, int num) {
    return value.get() / kWay;
  }

  public Configuration getConf() {
    return null;
  }

  public void setConf(Configuration conf) {
    kWay  = Integer.parseInt(conf.get("KWAY", "2"));
    System.out.println("kWay = " + kWay);    
  }
}