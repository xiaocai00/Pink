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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KruskalMain extends Configured implements Tool {

  static {
    String confDir = "/Users/xiaocai/hadoop/hadoop-1.0.4/conf/";

    Configuration.addDefaultResource(confDir + "core-site-local.xml");
    Configuration.addDefaultResource(confDir + "hdfs-site-local.xml");
    Configuration.addDefaultResource(confDir + "mapred-site-local.xml");

    /*
     * Configuration.addDefaultResource(confDir + "core-site.xml");
     * Configuration.addDefaultResource(confDir + "hdfs-site.xml");
     * Configuration.addDefaultResource(confDir + "mapred-site.xml");
     */

  }

  public static void main(String[] args) throws Exception {
    Configuration jobConf = new Configuration();
    String[] remainingArgs = new GenericOptionsParser(jobConf, args).getRemainingArgs();
    int res = ToolRunner.run(jobConf, new KruskalMain(), remainingArgs);
    System.exit(res);
  }

  public static void runKruskal(Configuration jobConf, int numParts, int kWay, Path pathIn, Path pathOut,
      String outdir) throws IOException, InterruptedException, ClassNotFoundException {
    int numReducers = 1;
    while (numParts > 1) {
      int remainder = numParts % kWay;
      if (remainder != 0 && remainder <= (kWay/2)) {
        numReducers = numParts / (kWay + 1);
        jobConf.set("KWAY", String.format("%s", kWay + 1));
      } else {
        numReducers = numParts / kWay;
        jobConf.set("KWAY", String.format("%s", kWay));
      }
      
      Job job = new Job(jobConf, "KruskalMain");
      job.setNumReduceTasks(numReducers);
      System.out.println("numParts = " + numParts);

      pathIn = pathOut;
      pathOut = new Path(outdir + "_p" + numParts);
      FileInputFormat.setInputPaths(job, pathIn);
      FileOutputFormat.setOutputPath(job, pathOut);
      System.out.println("input dir: " + pathIn.getParent() + ", " + pathIn.getName());
      System.out.println("output dir: " + pathOut.getParent() + ", " + pathOut.getName());

      job.setInputFormatClass(KruskalFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setMapperClass(KruskalMapper.class);
      job.setReducerClass(KruskalReducer.class);

      job.setMapOutputKeyClass(EdgeWritable.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(EdgeWritable.class);

      job.setPartitionerClass(KruskalPartitioner.class);
      if (job.waitForCompletion(true)) {
        System.out.println("Kruskal Main succeed!\n");
      } else {
        System.out.println("Kruskal Main doesn't succeed!\n");
        System.exit(-1);
      }
      numParts = numReducers;
    }
  }

  public int run(String[] args) throws Exception {
    Configuration jobConf = getConf();
    if (args.length < 5) {
      System.err.println("Usage: KruskalMain <in> <out> <numVertex> <numPartition> <kWay>");
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }

    jobConf.set("mapred.jar",
      "/Users/xiaocai/Documents/workspace/pink/pink/target/pink-0.0.1-SNAPSHOT.jar");
    Path pathIn = new Path(args[0]);
    Path pathOut = new Path(args[1]);
    jobConf.set("VertexNumber", args[2]);
    jobConf.set("ChunkNumber", args[3]);
    jobConf.set("KWay", args[4]);
    int numChunks = Integer.parseInt(args[3]);
    int kWay = Integer.parseInt(args[4]);
    int numParts = (numChunks * (numChunks - 1)) >> 1;
    runKruskal(jobConf, numParts, kWay, pathIn, pathOut, args[1]);
    return 0;
  }
}
