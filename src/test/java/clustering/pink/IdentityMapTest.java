/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class IdentityMapTest {
  MapDriver<IntWritable, Point, IntWritable, Point> mapDriver;
  @Before
  public void setUp() {
    IdentityMapper mapper = new IdentityMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  }
  
  @Test
  public void testMapper() {
    double[] vec = {0.0, 0.0};
    mapDriver.withInput(new IntWritable(1), new Point(0, vec));
    mapDriver.withOutput(new IntWritable(1), new Point(0, vec));
    //mapDriver.runTest();
  }
}
