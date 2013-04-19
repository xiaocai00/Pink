/******************************************************************************
 * Project: pink
 * Purpose: Hierarchical Single-linkage Clustering with MapReduce
 * Author : Chen Jin, karen.cj@gmail.com
 * ****************************************************************************
 * copyright (c) 2013, Chen Jin
 * all rights reserved
 *****************************************************************************/
package clustering.pink;

public class UnionFind {
  private int[] id;

  public UnionFind(int N) {
    if (N < 0) throw new IllegalArgumentException();
    id = new int[N];
    for (int i = 0; i < N; i++) {
      id[i] = i;
    }
  }
  
  public int find(int p) {
    while (p != id[p]) {
      p = id[p];
    }
    return p;
  }
  
  public boolean unify(int left, int right) {
    int i = find(left);
    int j = find(right);
    int big = (i > j) ? i : j;
    compressPath(left, big);
    compressPath(right, big);
    if (i == j) {
      return false;
    } else {
      return true;
    }
  }
  
  void compressPath(int element, int big) {
    int p = element, parent;
    while (id[p] != p) {
      parent = id[p];
      id[p] = big;
      p = parent;
    }
    id[p] = big;
  }
}
