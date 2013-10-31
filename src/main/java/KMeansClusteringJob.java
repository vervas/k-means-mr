import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

@SuppressWarnings("deprecation")
public class KMeansClusteringJob {

  private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    int iteration = 1;
    Configuration conf = new Configuration();
    conf.set("num.iteration", iteration + "");

    Path in = new Path("files/clustering/import/data");
    Path center = new Path("files/clustering/import/center/cen.seq");
    conf.set("centroid.path", center.toString());
    Path out = new Path("files/clustering/depth_1");

    Job job = new Job(conf);
    job.setJobName("KMeans Clustering");

    job.setMapperClass(KMeansMapper.class);
    job.setReducerClass(KMeansReducer.class);
    job.setJarByClass(KMeansMapper.class);

    FileInputFormat.addInputPath(job, in);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(out)) {
      fs.delete(out, true);
    }

    if (fs.exists(center)) {
      fs.delete(out, true);
    }

    if (fs.exists(in)) {
      fs.delete(in, true);
    }

    FileOutputFormat.setOutputPath(job, out);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setOutputKeyClass(Cluster.class);
    job.setOutputValueClass(Writable.class);

    job.waitForCompletion(true);

    long counter = job.getCounters()
        .findCounter(KMeansReducer.Counter.CONVERGED).getValue();
    iteration++;
    while (counter > 0) {
      conf = new Configuration();
      conf.set("centroid.path", center.toString());
      conf.set("num.iteration", iteration + "");
      job = new Job(conf);
      job.setJobName("KMeans Clustering " + iteration);

      job.setMapperClass(KMeansMapper.class);
      job.setReducerClass(KMeansReducer.class);
      job.setJarByClass(KMeansMapper.class);

      in = new Path("files/clustering/depth_" + (iteration - 1) + "/");
      out = new Path("files/clustering/depth_" + iteration);

      FileInputFormat.addInputPath(job, in);
      if (fs.exists(out))
        fs.delete(out, true);

      FileOutputFormat.setOutputPath(job, out);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setOutputKeyClass(Cluster.class);
      job.setOutputValueClass(Writable.class);

      job.waitForCompletion(true);
      iteration++;
      counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED)
          .getValue();
    }

    Path result = new Path("files/clustering/depth_" + (iteration - 1) + "/");

    FileStatus[] stati = fs.listStatus(result);
    for (FileStatus status : stati) {
      if (!status.isDir()) {
        Path path = status.getPath();
        if (!path.getName().equals("_SUCCESS")) {
          LOG.info("FOUND " + path.toString());
          try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
              conf)) {
            double[] key = {};
            double[] value = {};
            while (reader.next(key, value)) {
              LOG.info(key + " / " + value);
            }
          }
        }
      }
    }
  }

}
