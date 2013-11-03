import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
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

        Path in = new Path("/clustering/import/data");
        Path center = new Path("/clustering/import/center/cen.seq");
        conf.set("centroid.path", center.toString());
        Path out = new Path("/clustering/depth_1");

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

        writeExampleCenters(conf, center, fs);

        writeExampleVectors(conf, in, fs);

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Vector.class);
        job.setOutputValueClass(Vector.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        iteration++;
       // while (counter > 0 && iteration < 5) {
            conf = new Configuration();
            conf.set("centroid.path", center.toString());
            conf.set("num.iteration", iteration + "");
            job = new Job(conf);
            job.setJobName("KMeans Clustering " + iteration);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(KMeansMapper.class);

            in = new Path("/clustering/depth_" + (iteration - 1) + "/");
            out = new Path("/clustering/depth_" + iteration);

            FileInputFormat.addInputPath(job, in);
            if (fs.exists(out))
                fs.delete(out, true);

            FileOutputFormat.setOutputPath(job, out);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Vector.class);
            job.setOutputValueClass(Vector.class);

            job.waitForCompletion(true);
            iteration++;
            counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED)
                    .getValue();
       // }

        Path result = new Path("/clustering/depth_" + (iteration - 1) + "/");

        FileStatus[] statuses = fs.listStatus(result);
        for (FileStatus status : statuses) {
            if (!status.isDir()) {
                Path path = status.getPath();
                if (!path.getName().equals("_SUCCESS")) {
                    LOG.info("FOUND " + path.toString());
                    try {
                        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
                        Vector key = new Vector();
                        Vector v = new Vector();
                        while (reader.next(key, v)) {
                            LOG.info(key + " / " + v);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void writeExampleVectors(Configuration conf, Path in,
                                           FileSystem fs) throws IOException {
        try {
            SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Vector.class, Vector.class);
            for (int i = 0; i < 1000; i++) {
                double[] random = new double[10];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                dataWriter.append(new Vector(new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}), new Vector(random));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void writeExampleCenters(Configuration conf, Path center, FileSystem fs) throws IOException {
        try {
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Vector.class, IntWritable.class);

            final IntWritable value = new IntWritable(0);
            for (int i = 0; i < 10; i++) {
                double[] random = new double[10];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                centerWriter.append(new Vector(random), value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
