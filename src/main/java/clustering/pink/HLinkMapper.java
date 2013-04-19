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

public class HLinkMapper extends Mapper<IntWritable, Point, IntWritable, Point> {
  private int numChunks;
  private int numVertices;

  private IntWritable keyOut = new IntWritable();
  
  public int getChunkId(int vid) {
    // numChunks = 4;
    // numVertices = 8;
    int rowSize = numVertices / numChunks;
    int divid = numVertices % numChunks;
    int chunkId = 0;
    if (vid < divid * (rowSize + 1)) {
      chunkId = vid / (rowSize + 1);
    } else {
      chunkId = divid + (vid - divid * (rowSize + 1)) / rowSize;
    }
    return chunkId;
  }

  public int getPartId(int selfId, int otherId) {
    int big = Math.max(selfId, otherId);
    int small = Math.min(selfId, otherId);
    int partId = (big * (big - 1) >> 1) + small;
    System.out.println("\t" + selfId + ", " + otherId + ", " + partId);
    return partId;
  }

  protected void map(IntWritable key, Point value, Context context) throws IOException,
      InterruptedException {
    int selfId = getChunkId(key.get());
    for (int otherId = 0; otherId < numChunks; otherId++) {
      if (otherId == selfId) {
        continue;
      }      
      keyOut.set(getPartId(selfId, otherId));
      context.write(keyOut, value);
      System.out.println("KV = " + keyOut + ", " + value+"\n");
    }
  }
  
  public void setup(Context context) {
    numChunks = Integer.parseInt(context.getConfiguration().get("ChunkNumber"));
    numVertices = Integer.parseInt(context.getConfiguration().get("VertexNumber"));
    System.out.println("numChunks = " + numChunks);    
    System.out.println("numVertices = " + numVertices);
  }
}