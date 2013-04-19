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
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Implement the sequenceFileReader Kruskal Merging Phase
 */
public class HLinkRecordReader extends
    RecordReader<IntWritable, Point> {
 
  private int npts;
  private int ndims;
  private int currCnt = 0;
  private ByteBuffer buffer;
  private IntWritable currKey = new IntWritable();
  private Point currValue = new Point();
  private byte[] result;
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    FSDataInputStream in = null;

    try {
      FileSystem fs = FileSystem.get(conf);
      int fileLength = (int) split.getLength();
      result = new byte[fileLength];
      System.out.println("------filename: " + ((FileSplit) split).getPath());
      in = fs.open(((FileSplit) split).getPath());
      IOUtils.readFully(in, result, 0, fileLength);
      buffer = ByteBuffer.wrap(result);
      npts = buffer.getInt();
      ndims = buffer.getInt();
      System.out.println("npts, ndims: " + npts + ", " + ndims);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currCnt++ >= npts) {
      return false;
    }
    currValue.readFields(buffer, ndims);
    currKey.set(currValue.getId());
    return true;
  }

  @Override
  public IntWritable getCurrentKey() throws IOException, InterruptedException {
    return currKey;
  }

  @Override
  public Point getCurrentValue() throws IOException,
      InterruptedException {
    return currValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
  }
}
