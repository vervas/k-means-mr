import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class KMeansClusteringJob extends Configured implements Tool {

    private final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);
    private final int MB = 1024 * 1024;
    private final int BLOCK_SIZE;
    private final int CLUSTER_CENTERS;
    private final int MAX_ITERATIONS;

    private List<Vector> clusterCenters = new ArrayList<Vector>();

    public KMeansClusteringJob(String[] args) {
        int block_size = 32 * MB;
        int cluster_centers = 10;
        int max_iterations = 5;
        try {
            if (args.length > 2) block_size = Integer.parseInt(args[2]) * MB;
        } catch (NumberFormatException e) {
            LOG.error("Invalid block size argument. Using default: " + block_size);
        }

        try {
            if (args.length > 3) cluster_centers = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            LOG.error("Invalid clusters number argument. Using default: " + cluster_centers);
        }

        try {
            if (args.length > 4) max_iterations = Integer.parseInt(args[4]);
        } catch (NumberFormatException e) {
            LOG.error("Invalid max iterations argument. Using default: " + max_iterations);
        }

        BLOCK_SIZE = block_size;
        CLUSTER_CENTERS = cluster_centers;
        MAX_ITERATIONS = max_iterations;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new KMeansClusteringJob(args), args);
    }

    public int run(String[] args) throws Exception {
        int iteration = 0;

        Configuration conf = getConf();
        conf.setInt("dfs.blocksize", BLOCK_SIZE);
        Path center = new Path("/clustering/import/center/cen.seq");
        conf.set("centroid.path", center.toString());

        String target = "/clustering/import/data";

        Path dataSource = new Path(args[0]);
        Path in = new Path(target);
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(dataSource)) {
            writeVectors(conf, dataSource, in, Integer.parseInt(args[1]));
            writeCenters(conf, center);
        }

        while (iteration < MAX_ITERATIONS) {
            LOG.info("========Iteration: " + iteration);
            conf = getConf();
            conf.set("num.iteration", iteration + "");
            conf.set("centroid.path", center.toString());
            conf.setInt("dfs.block.size", BLOCK_SIZE);
            fs = FileSystem.get(conf);

            String inputPath = (iteration == 0) ? target : "/clustering/depth_" + (iteration - 1) + "/";

            in = new Path(inputPath);
            Path out = new Path("/clustering/depth_" + iteration);

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

        printCenters(center, conf);
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

    public void printCenters(Path path, Configuration conf) throws IOException {
        LOG.info("FOUND " + path.toString());
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
            Text key = new Text();
            Vector value = new Vector();
            while (reader.next(key, value)) {
                LOG.info(key + "\t/ " + value);
            }
        } catch (Exception e) {
            System.out.println("==========Error out");
            e.printStackTrace();
        }
        LOG.info("========Done printing");
    }

    public void writeVectors(Configuration conf, Path dataSource, Path target, int dataSize) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(target)) {
            fs.delete(target, true);
        }

        BufferedReader br = null;
        SequenceFile.Writer dataWriter = null;
        String line;
        String delimiter = ",";

        try {
            dataWriter = SequenceFile.createWriter(conf, Writer.file(target),
                    Writer.keyClass(Text.class), Writer.valueClass(Vector.class));

            br = new BufferedReader(new InputStreamReader(fs.open(dataSource)));
            int[] candidates = new int[CLUSTER_CENTERS];
            for (int j = 0; j < candidates.length; j++) {
                candidates[j] = 1 + (int) (Math.random() * dataSize);
            }

            int i = 0;
            br.readLine();
            while ((line = br.readLine()) != null && (i++ < dataSize)) {
                String[] values = line.split(delimiter);
                double[] vector = new double[5];

                for (int j = 0; j < 5; j++) {
                    if (values[j + 3].length() < 6) continue;
                    double value = Integer.parseInt(values[j + 3].substring(3)) * Math.random();
                    vector[j] = value;
                }

                Vector newVector = new Vector(vector);

                for (int candidate : candidates) {
                    if (i == candidate) clusterCenters.add(newVector);
                }

                dataWriter.append(new Text(""), newVector);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    dataWriter.close();
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeCenters(Configuration conf, Path center) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(center)) {
            fs.delete(center, true);
        }

        try {
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, Writer.file(center),
                    Writer.keyClass(Text.class), Writer.valueClass(Vector.class));

            int i = 0;
            for (Vector cluster : clusterCenters) {
                centerWriter.append(new Text("cluster" + i++), cluster);
            }
            centerWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
