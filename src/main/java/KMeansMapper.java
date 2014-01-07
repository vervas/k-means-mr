import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends  Mapper<Text, Vector, Text, Vector> {

    private final List<ClusterCenter> clusterCenters = new ArrayList<ClusterCenter>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(centroids));
        Text key = new Text();
        Vector value = new Vector();
        while (reader.next(key, value)) {
            ClusterCenter clusterCenter = new ClusterCenter(new Text(key), new Vector(value));
            clusterCenters.add(clusterCenter);
        }
        reader.close();
    }

    @Override
    protected void map(Text key, Vector value, Context context) throws IOException, InterruptedException {
        ClusterCenter nearest = clusterCenters.get(0);
        double nearestDistance = Double.MAX_VALUE;
        for (ClusterCenter clusterCenter : clusterCenters) {
            double dist = clusterCenter.measureDistance(value.getVector());
            if (nearestDistance > dist) {
                    nearest = clusterCenter;
                    nearestDistance = dist;
            }
        }

        context.write(nearest.getName(), value);
    }
}
