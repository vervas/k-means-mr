import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeansClusteringJob extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new KMeansClusteringJob(), null);
    }

    public int run(String[] arg0) throws Exception {

        int iteration = 1;

        Configuration conf = getConf();
        conf.set("num.iteration", iteration + "");
        conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");

        Path in = new Path("/clustering/import/data");
        Path center = new Path("/clustering/import/center/cen.seq");
        conf.set("centroid.path", center.toString());
        Path out = new Path("/clustering/depth_1");

        Job job = Job.getInstance(conf);
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

        writeExampleCenters(conf, center);

        writeExampleVectors(conf, in);

        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Vector.class);
        job.setOutputValueClass(Vector.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        iteration++;
        while (counter > 0 && iteration < 5) {
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
            counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
        }


        Path result = new Path("files/clustering/depth_" + (iteration - 1) + "/");

        FileStatus[] statuses = fs.listStatus(result);
        for (FileStatus status : statuses) {
            if (!status.isDirectory()) {
                Path path = status.getPath();
                if (!path.getName().equals("_SUCCESS")) {
                    LOG.info("FOUND " + path.toString());
                    try {
                        SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
                        Vector key = new Vector();
                        Vector value = new Vector();
                        while (reader.next(key, value)) {
                            LOG.info(key + " / " + value);
                        }
                    } catch (Exception e) {
                        System.out.println("==========Error out");
                        e.printStackTrace();
                    }
                }
            }
        }

        return 0;
    }

    public static void writeExampleVectors(Configuration conf, Path in) throws IOException {
        try {
            SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf, Writer.file(in),
                    Writer.keyClass(Vector.class), Writer.valueClass(Vector.class));

            for (int i = 0; i < 100; i++) {
                double[] random = new double[5];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                dataWriter.append(new Vector(new double[]{0, 0, 0, 0, 0}), new Vector(random));
            }
            dataWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void writeExampleCenters(Configuration conf, Path center) throws IOException {
        try {
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, Writer.file(center),
                    Writer.keyClass(Vector.class), Writer.valueClass(IntWritable.class));

            final IntWritable value = new IntWritable(0);
            for (int i = 0; i < 10; i++) {
                double[] random = new double[5];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                centerWriter.append(new Vector(random), value);
            }
            centerWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
