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

public class KruskalReducer extends Reducer<EdgeWritable, IntWritable, IntWritable, EdgeWritable> {
  private UnionFind uf;
  private int kWay = 2;
  private IntWritable keyOut = new IntWritable();
  protected void reduce(EdgeWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    for (IntWritable val : values) {
      System.out.println("edgeIn: " + val + ", " + key);
      if (uf.unify(key.getLeft(), key.getRight())) {
        keyOut.set(val.get()/kWay) ; 
        context.write(keyOut, key);
        System.out.println("edgeOut: " + val + ", " + keyOut);
      }
    }
  }

  public void setup(Context context) {
    int numVertices = Integer.parseInt(context.getConfiguration().get("VertexNumber"));
    kWay  = Integer.parseInt(context.getConfiguration().get("KWAY", "2"));
    System.out.println("kWay = " + kWay);   
    uf = new UnionFind(numVertices);
  }
}
