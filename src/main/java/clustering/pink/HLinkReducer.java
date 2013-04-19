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

public class HLinkReducer extends Reducer<IntWritable, Point, IntWritable, EdgeWritable> {

  private int[] globalId = null;
  private Point[] database = null;

  protected void reduce(IntWritable key, Iterable<Point> values, Context context)
      throws IOException, InterruptedException {
    
    int numPts = 0;
    for (Point value : values) {
      globalId[numPts] = value.getId();
      database[numPts] = new Point(value);
      System.out.println("KV: " + key + ", " + globalId[numPts] + ", " + database[numPts] );
      numPts++;
    }
    
    PrimLocal prim = new PrimLocal(database, numPts);
    prim.findMST(globalId);
    for (EdgeWritable edge : prim.getEdges()) {
      context.write(key, edge);
      //System.out.println("key: " + key + " -- " + edge);
    }
  }

  public void setup(Context context) {
    int numChunks = Integer.parseInt(context.getConfiguration().get("ChunkNumber"));
    int numVertices = Integer.parseInt(context.getConfiguration().get("VertexNumber"));
    int partSize = ((numVertices + numChunks - 1) / numChunks) << 1;
    database = new Point[partSize];
    globalId = new int[partSize];
  }
}
