import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
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

        writeExampleCenters(conf, center, 10);
        writeExampleVectors(conf, in);

        while (iteration < 5) {
            LOG.info("========Iteration: " + iteration);
            conf = getConf();
            conf.set("num.iteration", iteration + "");
            conf.set("centroid.path", center.toString());
            FileSystem fs = FileSystem.get(conf);

            inputFile = (iteration == 0) ? inputFile : "/clustering/depth_" + (iteration - 1) + "/part-r-00000";

            in = new Path(inputFile);
            Path out = new Path("/clustering/depth_" + iteration);

            printCenters(center, conf);
            printVectors(in, conf);

            Job job = Job.getInstance(conf);
            job.setJobName("KMeans Clustering " + iteration);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(KMeansMapper.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, in);

            if (fs.exists(out)) {
                fs.delete(out, true);
            }
            FileOutputFormat.setOutputPath(job, out);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Vector.class);

            LOG.info("========Before job " + iteration);

            job.waitForCompletion(true);

            if (job.isSuccessful()) {
                LOG.info("========Done iter: " + iteration);
            } else {
                break;
            }

            iteration++;
        }

        saveResult(new Path("/clustering/depth_" + (iteration - 1) + "/part-r-00000"), conf);

        return 0;
    }

    private void saveResult(Path out, Configuration conf) throws IOException {
        LOG.info("FOUND " + out.toString());
        FileSystem fs = FileSystem.get(conf);

        Path result = new Path("/clustering/result");
        if (fs.exists(result)) {
            fs.delete(result, true);
        }

        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(out));
            FSDataOutputStream resultStream = fs.create(result);
            Text key = new Text();
            Vector value = new Vector();
            while (reader.next(key, value)) {
                resultStream.writeBytes(key + "\t / " + value + "\n");
            }
        } catch (Exception e) {
            System.out.println("==========Error out");
            e.printStackTrace();
        }
        LOG.info("========DONE");
    }

    public static void printCenters(Path path, Configuration conf) throws IOException {
        LOG.info("FOUND " + path.toString());
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
            Text key = new Text();
            Vector value = new Vector();
            int i = 0;
            while (reader.next(key, value) && i++ < 10) {
                LOG.info(key + "\t/ " + value);
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
            Text key = new Text();
            Vector value = new Vector();
            int i = 0;
            while (reader.next(key, value) && i++ < 10) {
                LOG.info(key + "\t / " + value);
            }
        } catch (Exception e) {
            System.out.println("==========Error out");
            e.printStackTrace();
        }
        LOG.info("========Done printing");
    }

    public static void writeExampleVectors(Configuration conf, Path in) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(in)) {
            fs.delete(in, true);
        }

        try {
            SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf, Writer.file(in),
                    Writer.keyClass(Text.class), Writer.valueClass(Vector.class));

            for (int i = 0; i < 1000; i++) {
                double[] random = new double[5];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                dataWriter.append(new Text(""), new Vector(random));
            }
            dataWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void writeExampleCenters(Configuration conf, Path center, int clusters) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(center)) {
            fs.delete(center, true);
        }

        try {
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, Writer.file(center),
                    Writer.keyClass(Text.class), Writer.valueClass(Vector.class));

            for (int i = 0; i < clusters; i++) {
                double[] random = new double[5];
                for (int j = 0; j < random.length; j++) {
                    random[j] = (int) (Math.random() * (100 + 1));
                }
                centerWriter.append(new Text("cluster" + i), new Vector(random));
            }
            centerWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
