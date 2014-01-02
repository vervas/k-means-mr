import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends  Mapper<Text, Vector, Text, Vector> {

    private final List<Cluster> clusters = new ArrayList<Cluster>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(centroids));
        Text key = new Text();
        Vector value = new Vector();
        while (reader.next(key, value)) {
            Cluster cluster = new Cluster(new Text(key), new Vector(value));
            clusters.add(cluster);
        }
        reader.close();
    }

    @Override
    protected void map(Text key, Vector value, Context context) throws IOException, InterruptedException {
        Cluster nearest = clusters.get(0);
        double nearestDistance = Double.MAX_VALUE;
        for (Cluster cluster : clusters) {
            double dist = cluster.getCenter().measureDistance(value.getVector());
            if (nearestDistance > dist) {
                    nearest = cluster;
                    nearestDistance = dist;
            }
        }

        context.write(nearest.getName(), value);
    }
}
