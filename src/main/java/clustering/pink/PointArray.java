/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class PointArray extends ArrayWritable{

  public PointArray(Class<? extends Writable> valueClass) {
    super(valueClass);
  }

  public PointArray() {
    // TODO Auto-generated constructor stub
  }

}
