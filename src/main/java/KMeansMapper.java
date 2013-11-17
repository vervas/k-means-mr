import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;


// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends Mapper<Vector, Vector, Vector, Vector> {

    private final List<Vector> centers = new ArrayList();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));

        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(centroids));
            Vector key = new Vector();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                Vector clusterCenter = new Vector(key);
                centers.add(clusterCenter);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(Vector key, Vector value, Context context) throws IOException, InterruptedException {
        Vector nearest = null;
        double nearestDistance = Double.MAX_VALUE;
        for (Vector c : centers) {
            double dist = c.measureDistance(value.getVector());
            if (nearest == null) {
                nearest = c;
                nearestDistance = dist;
            } else {
                if (nearestDistance > dist) {
                    nearest = c;
                    nearestDistance = dist;
                }
            }
        }
        context.write(nearest, value);
    }


}
