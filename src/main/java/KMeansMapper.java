import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
@SuppressWarnings("deprecation")
public class KMeansMapper extends
		Mapper<Cluster, Writable, Cluster, Writable> {

	private final List<Cluster> centers = new ArrayList<>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				centroids, conf)) {
			double[] key = {};
			double[] value = {};
			int index = 0;
			while (reader.next(key, value)) {
				Cluster clusterCenter = new Cluster(key);
				clusterCenter.setClusterIndex(index++);
				centers.add(clusterCenter);
			}
		}
	}

	@Override
	protected void map(Cluster key, Writable value, Context context)
			throws IOException, InterruptedException {

		Cluster nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		for (Cluster c : centers) {
			double dist = measureDistance(c.getCenterVector(),
					null);
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

		  public double measureDistance(double[] set1, double[] set2) {
			    double sum = 0;
			    int length = set1.length;
			    for (int i = 0; i < length; i++) {
			      double diff = set2[i] - set1[i];
			      // multiplication is faster than Math.pow() for ^2.
			      sum += (diff * diff);
			    }

			    return Math.sqrt(sum);
			  }
}
