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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CanopyPrimMapper extends Mapper<IntWritable, Point,  EdgeWritable, IntWritable> {
  List<Point> database = new ArrayList<Point>();
  List<Integer> globalID = new ArrayList<Integer>();
  
  protected void map(IntWritable key, Point value, Context context) throws IOException,
      InterruptedException {
    database.add(new Point(value));
    globalID.add(new Integer(value.getId()));    
    PrimLocal mst = new PrimLocal(database.toArray(new Point[0]), database.size());
    mst.findMST(globalID.toArray(new Integer[0]));
    for (EdgeWritable edge : mst.getEdges()) {
      context.write(edge, key);
    }
  }
  
  public void setup(Context context) {
  }
}