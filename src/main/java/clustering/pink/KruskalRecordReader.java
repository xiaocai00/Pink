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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Implement the sequenceFileReader Kruskal Merging Phase
 */
public class KruskalRecordReader extends RecordReader<IntWritable, EdgeWritable> {

  private IntWritable currKey = new IntWritable();
  private EdgeWritable currValue = new EdgeWritable();
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
    boolean ret = reader.next(currKey, currValue);
    System.out.println("reader = " + currKey + ", " + currValue);
    return ret;
  }

  @Override
  public IntWritable getCurrentKey() throws IOException, InterruptedException {
    return currKey;
  }

  @Override
  public EdgeWritable getCurrentValue() throws IOException, InterruptedException {
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
