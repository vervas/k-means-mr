import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


// calculate a new clustercenter for these vertices
public class KMeansReducer extends
		Reducer<Cluster, Writable, Cluster, Writable> {

	public static enum Counter {
		CONVERGED
	}

	private final List<double[]> centers = new ArrayList<>();

	@Override
	protected void reduce(Cluster key, Iterable<Writable> values,
			Context context) throws IOException, InterruptedException {

		List<Writable> vectorList = new ArrayList<>();
		double[] newCenter = null;
		for (Writable value : values) {
			vectorList.add(new IntWritable(0));
			if (newCenter == null)
				newCenter = null;
			else
//				newCenter = newCenter.add(value.readFields(null));
		}

		newCenter = newCenter.divide(vectorList.size());
		ClusterCenter center = new ClusterCenter(newCenter);
		centers.add(center);
		for (VectorWritable vector : vectorList) {
			context.write(center, vector);
		}

		if (center.converged(key))
			context.getCounter(Counter.CONVERGED).increment(1);

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		try (SequenceFile.Writer out = SequenceFile.createWriter(fs,
				context.getConfiguration(), outPath, ClusterCenter.class,
				IntWritable.class)) {
			final IntWritable value = new IntWritable(0);
			for (double[] center : centers) {
				out.append(center, value);
			}
		}
	}
}
