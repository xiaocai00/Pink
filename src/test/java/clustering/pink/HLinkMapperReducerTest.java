/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class HLinkMapperReducerTest {
  Configuration conf = new Configuration();
  MapDriver<IntWritable, Point, IntWritable, Point> mapDriver;
  ReduceDriver<IntWritable, Point, IntWritable, EdgeWritable> reduceDriver;
  //MapReduceDriver<IntWritable, Point, IntWritable, Point, IntWritable, EdgeWritable> mapReduceDriver;

  @Before
  public void setUp() {
    HLinkMapper mapper = new HLinkMapper();
    HLinkReducer reducer = new HLinkReducer();
   
    mapDriver = MapDriver.newMapDriver(mapper);
    mapDriver.setConfiguration(conf);
    conf.set("ChunkNumber", "4");
    conf.set("VertexNumber", "10");
    
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    reduceDriver.setConfiguration(conf);

    //mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test

  public void testMapper() {
    int npt = 1;
    double[][] vec = new double[npt][2];
    for (int i = 0; i < npt; i++) {
      vec[i] = new double[2];
      for (int j = 0; j < 2; j++) {
        vec[i][j] = i;
      }
    }
    for (int i = 0; i < npt; i++){
      mapDriver.withInput(new IntWritable(i), new Point(i, vec[i]));
    }
    
  int[][] partId = {{0, 1, 3}, {0, 2, 4}, {1, 2, 5}, {3, 4, 5}};

   for (int i = 0; i < npt; i++){
     for (int j = 0; j < 3; j++) 
       mapDriver.withOutput(new IntWritable(partId[i][j]), new Point(i, vec[i]));
   }
   //mapDriver.runTest();
  }

  @Test
  public void testReducer() {
     
    int npt = 4;
    double[][] vec = new double[npt][2];
    double multiplier = 4;
    List<Point> values = new ArrayList<Point>();
    for (int i = 0; i < npt; i++) {
      vec[i] = new double[2];
      for (int j = 0; j < 2; j++) {
        vec[i][j] = i * multiplier;
        multiplier = multiplier/2;
      }
      values.add(new Point(i, vec[i]));
    }
    
    reduceDriver.withInput(new IntWritable(0), values);
    reduceDriver.withOutput(new IntWritable(0), new EdgeWritable());
    //reduceDriver.runTest();
    
  }
}