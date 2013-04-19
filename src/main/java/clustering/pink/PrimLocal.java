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

public class PrimLocal {

  private int npts;
  private Point[] data;
  private EdgeWritable[] edges;

  PrimLocal(Point[] dataIn, int n) {
    if (n <= 0 || n > dataIn.length) {
      throw new IllegalArgumentException();
    }
    npts = n;
    data = dataIn;    
  }
  
  public EdgeWritable[] findMST(Integer[] globalID) {
    edges = new EdgeWritable[npts - 1];
    int[] left = new int[npts];
    int[] parent = new int[npts];
    double[] key = new double[npts];
    
    for (int i = 0; i < npts; i++) {
      key[i] = Double.MAX_VALUE;
      left[i] = i + 1;
    }
    key[0] = 0;
    parent[0] = -1;
    
    int next = 0, shift, currPt, otherPt, last = 0;
    double minV, dist;
    // /////////////////////////////
    for (int j = npts - 1; j > 0;) {
      currPt = next;
      shift = 0;
      next = left[shift];
      minV = Double.MAX_VALUE;
      for (int i = 0; i < j; i++) {
        otherPt = left[i];
        try {
          dist = data[currPt].distanceTo(data[otherPt]);
          if (key[otherPt] > dist) {
            key[otherPt] = dist;
            parent[otherPt] = currPt;
          }
        } catch (Exception e) {
          System.err.println("curr, other: " + currPt + ", " + otherPt);
          e.printStackTrace();
        }
        if (key[otherPt] < minV) {
          shift = i;
          minV = key[otherPt];
          next = otherPt;
        }
      }
      //System.out.println("minV: "+minV + ", " + next + ", " + parent[next]);
      edges[last++] = new EdgeWritable(minV, Math.min(globalID[next],
          globalID[parent[next]]), Math.max(globalID[next],
          globalID[parent[next]]));
      left[shift] = left[--j];
    }
      
    // Arrays.sort(edges);
    return edges;
  }
  
  /**
   * PRIM Algorithm
   * @return the minimum spanning tree
   */
  public EdgeWritable[] findMST(int[] globalID) {
    edges = new EdgeWritable[npts - 1];
    int[] left = new int[npts];
    int[] parent = new int[npts];
    double[] key = new double[npts];
    
    for (int i = 0; i < npts; i++) {
      key[i] = Double.MAX_VALUE;
      left[i] = i + 1;
    }
    key[0] = 0;
    parent[0] = -1;
    
    int next = 0, shift, currPt, otherPt, last = 0;
    double minV, dist;
    // /////////////////////////////
    for (int j = npts - 1; j > 0;) {
      currPt = next;
      shift = 0;
      next = left[shift];
      minV = Double.MAX_VALUE;
      for (int i = 0; i < j; i++) {
        otherPt = left[i];
        try {
          dist = data[currPt].distanceTo(data[otherPt]);
          if (key[otherPt] > dist) {
            key[otherPt] = dist;
            parent[otherPt] = currPt;
          }
        } catch (Exception e) {
          System.err.println("curr, other: " + currPt + ", " + otherPt);
          e.printStackTrace();
        }
        if (key[otherPt] < minV) {
          shift = i;
          minV = key[otherPt];
          next = otherPt;
        }
      }
      //System.out.println("minV: "+minV + ", " + next + ", " + parent[next]);
      edges[last++] = new EdgeWritable(minV, Math.min(globalID[next],
          globalID[parent[next]]), Math.max(globalID[next],
          globalID[parent[next]]));
      left[shift] = left[--j];
    }
      
    // Arrays.sort(edges);
    return edges;
  }

  public void displayMST() {
    for (int i = 0; i < npts - 1; i++) {
      System.out.format("%8.2f %d %d\n", edges[i].getDistance(),
        edges[i].getLeft(), edges[i].getRight());
    }
  }

  public EdgeWritable[] getEdges() {
    return edges;
  }

  public boolean isMST() {
    if (edges == null) throw new IllegalArgumentException();
    return true;
  }

  public static void main(String[] args) throws IOException {
    int npts = 4;
    int ndims = 2;
    double[] vector = new double[ndims];
    Point[] data = new Point[npts];

    vector[0] = 0;
    vector[1] = 0;
    data[0] = new Point(0, vector);

    vector[0] = 2.0;
    vector[1] = 0;
    data[1] = new Point(1, vector);

    vector[0] = 1.0;
    vector[1] = 2.0;
    data[2] = new Point(2, vector);

    vector[0] = 1.1;
    vector[1] = 2.0;
    data[3] = new Point(3, vector);
    int[] globalId = new int[npts];
    for (int i = 0; i < npts; i++)
      globalId[i] = i;
    PrimLocal prim = new PrimLocal(data, data.length);
    prim.findMST(globalId);
    prim.displayMST();
  }
}