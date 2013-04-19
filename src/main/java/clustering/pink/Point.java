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
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements Writable, WritableComparable<Point> {
  private int gid = Integer.MAX_VALUE;;
  private double[] vec;

  public Point() {
  }
  
  public Point(Point other) {
    gid = other.gid;
    vec = other.vec.clone();
  }

  public Point(int gidIn, double[] vecIn) {
    gid = gidIn;
    vec = vecIn.clone();
  }

  public int getId() {
    return gid;
  }

  public double distanceTo(Point other) {
    double dist = 0;
    for (int i = 0; i < vec.length; i++) {
      dist += Math.pow(vec[i] - other.vec[i], 2);
    }
    return Math.pow(dist, 0.5);
  }

  public void readFields(DataInput in) throws IOException {
    gid = in.readInt();
    vec = new double[in.readInt()];
    for (int i = 0; i < vec.length; i++) {
      vec[i] = in.readDouble();
    }
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(gid);
    out.writeInt(vec.length);
    for (double v : vec) {
      out.writeDouble(v);
    }
  }
  
  @Override
  public String toString() {
    String ret = "" + gid;
    for (double v : vec) {
      ret += String.format(" %.2f", v);
    }
    return ret;
  }
  
  public void readFields(ByteBuffer buffer, int ndims) {
    gid = buffer.getInt();
    vec = new double[ndims];
    for (int i = 0; i < ndims; i++) {
      vec[i] = buffer.getDouble();
    }
  }

  public boolean equals(Point other) {
    System.out.println("compare");
    if (gid == other.gid) {
      for (int i = 0; i < vec.length; i++) {
        if (vec[i] != other.vec[i]) {
          return false;
        }
      }
      return true;
    } 
    return false;
  }

  @Override
  public int hashCode() {
    double sum = 0;
    for (double v : vec) {
      sum += v;
    }
    return gid * 163 + vec.length * 57 + (int) sum;
    //System.out.println("hashCode: " + code);
    //return code;
  }
  
  public int compareTo(Point other) {
    System.out.println("compare");
    if (gid == other.gid) {
      for (int i = 0; i < vec.length; i++) {
        if (vec[i] < other.vec[i]) return -1;
        else if (vec[i] > other.vec[i]) return 1;
      }
      return 0;
    } else {
      if (gid > other.gid) {
        return 1;
      } else if (gid == other.gid) {
        return 0;
      } else {
        return 1;
      }
    }
  }
}
