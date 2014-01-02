import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
        ToolRunner.run(new Configuration(), new KMeansClusteringJob(), null);
    }

    public int run(String[] arg0) throws Exception {
        int iteration = 0;
        Configuration conf = getConf();
        Path center = new Path("/clustering/import/center/cen.seq");
        conf.set("centroid.path", center.toString());

        String inputFile = "/clustering/import/data";

        Path in = new Path(inputFile);
        Path out = new Path("/clustering/depth_" + iteration);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        if (fs.exists(center)) {
            fs.delete(center, true);
        }

        if (fs.exists(in)) {
            fs.delete(in, true);
        }

        writeExampleCenters(conf, center);
        writeExampleVectors(conf, new Path("/clustering/import/data"));

        while (iteration < 5) {
            LOG.info("========Iteration: " + iteration);
            conf = getConf();
            conf.set("num.iteration", iteration + "");
            conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                    + "org.apache.hadoop.io.serializer.WritableSerialization");

            inputFile = (iteration == 0) ? "/clustering/import/data" : "/clustering/depth_" + (iteration - 1) + "/part-r-00000";

            in = new Path(inputFile);
            out = new Path("/clustering/depth_" + iteration);

            printCenters(center, conf);
            printVectors(in, conf);

            conf.set("centroid.path", center.toString());

            Job job = Job.getInstance(conf);
            job.setJobName("KMeans Clustering " + iteration);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(KMeansMapper.class);

            FileInputFormat.addInputPath(job, in);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            FileOutputFormat.setOutputPath(job, out);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(ClusterCenter.class);
            job.setOutputValueClass(Vector.class);

            LOG.info("========Before job " + iteration);

            job.waitForCompletion(true);

            if (job.isSuccessful()) {
                LOG.info("========Done iter: " + iteration);
            } else {
                break;
            }


            LOG.info("========Increasing iteration " + iteration);
            iteration++;
        }

        return 0;
    }

    public static void printCenters(Path path, Configuration conf) throws IOException {
        LOG.info("FOUND " + path.toString());
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
            ClusterCenter key = new ClusterCenter();
            IntWritable value = new IntWritable();
            int i = 0;
            while (reader.next(key, value) && i < 3) {
                LOG.info(key);
                i++;
            }
        } catch (Exception e) {
            System.out.println("==========Error out");
            e.printStackTrace();
        }
        LOG.info("========Done printing");
    }

    public static void printVectors(Path path, Configuration conf) throws IOException {
        LOG.info("FOUND " + path.toString());
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
            ClusterCenter key = new ClusterCenter();
            Vector value = new Vector();
            int i = 0;
            while (reader.next(key, value) && i < 3) {
                LOG.info(key + " / " + value);
                i++;
            }
        } catch (Exception e) {
            System.out.println("==========Error out");
            e.printStackTrace();
        }
        LOG.info("========Done printing");
    }

    public static void writeExampleVectors(Configuration conf, Path in) throws IOException {
        try {
            SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf, Writer.file(in),
                    Writer.keyClass(ClusterCenter.class), Writer.valueClass(Vector.class));

            for (int i = 0; i < 1000; i++) {
                double[] random = new double[5];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                dataWriter.append(new ClusterCenter(new double[]{0, 0, 0, 0, 0}), new Vector(random));
            }
            dataWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void writeExampleCenters(Configuration conf, Path center) throws IOException {
        try {
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, Writer.file(center),
                    Writer.keyClass(ClusterCenter.class), Writer.valueClass(IntWritable.class));

            final IntWritable value = new IntWritable(0);
            for (int i = 0; i < 10; i++) {
                double[] random = new double[5];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                centerWriter.append(new ClusterCenter(random), value);
            }
            centerWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
