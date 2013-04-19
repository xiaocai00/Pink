/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class HLinkTest {
  private HLinkMapper mapper;
  private Context context;
  final Map<Object,Object> test = new HashMap();
  private Configuration conf;

  /** the injected mapper context */
  public Context getContext() {
    return context;
  }
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();

      mapper = new HLinkMapper();

      //conf.set("ChunkNumber", "4");
      //conf.set("VertexNumber", "8");


  }
  /*
  @Test
  public void testMethod() throws IOException, InterruptedException {
      context = mock(Context.class);
      conf = mock(Configuration.class);
      
      when(conf.get("ChunkNumber")).thenReturn("4");      
      when(conf.get("VertexNumber")).thenReturn("8");
      
      doAnswer(new Answer<Object>() {
          public Object answer(InvocationOnMock invocation) {
              Object[] args = invocation.getArguments();
              test.put(args[0].toString(), args[1].toString());
              return "called with arguments: " + args;
          }
      }).when(context).write(any(IntWritable.class),any(IntWritable.class));
      
      
      int npt = 8;
      double[][] vec = new double[npt][2];
      for (int i = 0; i < npt; i++) {
        vec[i] = new double[2];
        for (int j = 0; j < 2; j++) {
          vec[i][j] = i;
        }
        mapper.map(new IntWritable(i), new Point(i, vec[i]), context);
      }
      //int[][] partId = {{0, 1, 3}, {0, 2, 4}, {1, 2, 5}, {3, 4, 5}};

      Map<String,String> actualMap = new HashMap<String, String>();
      actualMap.put("0", "0 0.00 0.00");
      actualMap.put("1", "0 0.00 0.00");
      actualMap.put("3", "0 0.00 0.00");
      
      actualMap.put("0", "1 1.00 1.00");
      actualMap.put("1", "1 1.00 1.00");
      actualMap.put("3", "1 1.00 1.00");
      
      actualMap.put("0", "2 2.00 2.00");
      actualMap.put("2", "2 2.00 2.00");
      actualMap.put("4", "2 2.00 2.00");
      
      actualMap.put("0", "3 3.00 3.00");
      actualMap.put("2", "3 3.00 3.00");
      actualMap.put("4", "3 3.00 3.00");
      
      actualMap.put("1", "4 4.00 4.00");
      actualMap.put("2", "4 4.00 4.00");
      actualMap.put("5", "4 4.00 4.00");
      
      actualMap.put("1", "5 5.00 5.00");
      actualMap.put("2", "5 5.00 5.00");
      actualMap.put("5", "5 5.00 5.00");
      
      actualMap.put("3", "6 6.00 6.00");
      actualMap.put("4", "6 6.00 6.00");
      actualMap.put("5", "6 6.00 6.00");
      
      actualMap.put("3", "7 7.00 7.00");
      actualMap.put("4", "7 7.00 7.00");
      actualMap.put("5", "7 7.00 7.00");

      assertEquals(actualMap, test);
  }
  
  
  
  */
}
