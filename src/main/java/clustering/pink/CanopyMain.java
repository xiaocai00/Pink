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

public class CanopyMain extends Configured implements Tool {

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
    int res = ToolRunner.run(jobConf, new HLinkMain(), remainingArgs);
    System.exit(res);
  }

  public static void runCanopy(Configuration jobConf, int numParts, Path pathIn, Path pathOut)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(jobConf, "CanopyMain");
    FileInputFormat.setInputPaths(job, pathIn);
    FileOutputFormat.setOutputPath(job, pathOut);
    
    // merge all the canopies and spits out one sequencefile per canopy
    // and form the pointArray
    job.setNumReduceTasks(1);
    job.setInputFormatClass(HLinkFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(CanopyMapper.class);
    job.setReducerClass(CanopyReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Point.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Point.class);

    //job.setPartitionerClass(CanopyPartitioner.class);
    System.out.println("numParts = " + numParts);
    if (job.waitForCompletion(true)) {
      System.out.println("HLink Main succeed!\n");
    } else {
      System.out.println("HLink Main doesn't succeed!\n");
      System.exit(-1);
    }
  }
  
  public static void runCanopyPrim(Configuration jobConf, int numParts, Path pathIn, Path pathOut)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(jobConf, "CanopyPrimMain");
    FileInputFormat.setInputPaths(job, pathIn);
    FileOutputFormat.setOutputPath(job, pathOut);

    job.setNumReduceTasks(numParts);

    job.setInputFormatClass(CanopyPrimFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(CanopyPrimMapper.class);
    job.setReducerClass(KruskalReducer.class);
    job.setPartitionerClass(KruskalPartitioner.class);
    job.setMapOutputKeyClass(EdgeWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(EdgeWritable.class);

    //job.setPartitionerClass(CanopyPartitioner.class);
    System.out.println("numParts = " + numParts);
    if (job.waitForCompletion(true)) {
      System.out.println("HLink Main succeed!\n");
    } else {
      System.out.println("HLink Main doesn't succeed!\n");
      System.exit(-1);
    }
  }

  public int run(String[] args) throws Exception {
    Configuration jobConf = getConf();
    if (args.length < 5) {
      System.err.println("Usage: HLinkMain <in> <out> <numVertex> <numPartition> <kWay>");
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }

    jobConf.set("mapred.jar",
      "/Users/xiaocai/Documents/workspace/pink/pink/target/pink-0.0.1-SNAPSHOT.jar");

    jobConf.set("VertexNumber", args[2]);
    jobConf.set("ChunkNumber", args[3]);
    jobConf.set("KWay", args[4]);
    
    int kWay = Integer.parseInt(args[4]);    
    int numChunks = Integer.parseInt(args[3]);
    int numParts = (numChunks * (numChunks - 1)) >> 1;
    
    Path pathIn = new Path(args[0]);
    Path pathOut = new Path(args[1]);
    runCanopy(jobConf, numParts, pathIn, pathOut);
    runCanopyPrim(jobConf, numParts, pathIn, pathOut);
    KruskalMain.runKruskal(jobConf, numParts, kWay, pathIn, pathOut, args[1]);
    return 0;
  }
}
