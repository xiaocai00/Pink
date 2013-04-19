/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class EdgeWritable implements WritableComparable<EdgeWritable> {
  
  private double dist = 0;
  private int left = -1;
  private int right = -1;
  
  public EdgeWritable(double distIn, int leftIn, int rightIn) {
    dist = distIn;
    left = leftIn;
    right = rightIn;
  }
  
  public EdgeWritable() {
    // TODO Auto-generated constructor stub
  }

  public void readFields(DataInput in) throws IOException {
    dist = in.readDouble();
    left = in.readInt();
    right = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeDouble(dist);
    out.writeInt(left);
    out.writeInt(right);
  }

  public double getDistance() {
    return dist;
  }
  
  public int getLeft() {
    return left;
  }
  
  public int getRight() {
    return right;
  }

  public int compareTo(EdgeWritable other) {
    if (this.dist < other.dist) {
      return -1;
    } else if (this.dist == other.dist) {
      return 0;
    } else {
      return 0;
    }
  }

  public String toString() {
    String ret = String.format("%.2f (%d %d)", dist, left, right);
    return ret;
  }
}
