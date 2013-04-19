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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CanopyPrimRecordReader extends RecordReader<IntWritable, PointArray> {

  private IntWritable currKey = new IntWritable();
  private PointArray currValue = new PointArray();
  private List<Point> holder = new ArrayList<Point>();
  private Point elem = new Point();
  private boolean status = true;
  private SequenceFile.Reader reader = null;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    System.out.println("------file: " + ((FileSplit) split).getPath());
    reader = new SequenceFile.Reader(fs, ((FileSplit) split).getPath(), conf);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader == null) {
      return false;
    } 
    while (reader.next(currKey, elem)) {
      holder.add(new Point(elem));
      System.out.println("reader = " + currKey + ", " + elem);
    }
    currValue.set(holder);
    return status;
    
  }

  @Override
  public IntWritable getCurrentKey() throws IOException, InterruptedException {
    return currKey;
  }

  @Override
  public PointArray getCurrentValue() throws IOException, InterruptedException {
    return currValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }
}