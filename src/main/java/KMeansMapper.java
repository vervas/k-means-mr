import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends  Mapper<ClusterCenter, Vector, ClusterCenter, Vector> {

    private final List<ClusterCenter> centers = new ArrayList<ClusterCenter>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));

        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(centroids));
            ClusterCenter key = new ClusterCenter();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                ClusterCenter clusterCenter = new ClusterCenter(key);
                centers.add(clusterCenter);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(ClusterCenter key, Vector value, Context context) throws IOException, InterruptedException {
        ClusterCenter nearest = centers.get(0);
        double nearestDistance = Double.MAX_VALUE;
        for (ClusterCenter c : centers) {
            double dist = c.measureDistance(value.getVector());
            if (nearestDistance > dist) {
                    nearest = c;
                    nearestDistance = dist;
            }
        }
        try {
            context.write(nearest, value);
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }
}
