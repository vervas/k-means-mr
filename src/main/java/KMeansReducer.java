import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;


// calculate a new Vector for these vertices
@SuppressWarnings("deprecation")
public class KMeansReducer extends Reducer<Vector, Vector, Vector, Vector> {

    public static enum Counter {
        CONVERGED
    }

    private final List<Vector> centers = new ArrayList();

    @Override
    protected void reduce(Vector key, Iterable<Vector> values, Context context) throws IOException, InterruptedException {

        Vector newCenter = new Vector();
        List<Vector> vectorList = new ArrayList();
        int vectorSize = key.getVector().length;
        newCenter.setVector(new double[vectorSize]);
        for (Vector value : values) {
            vectorList.add(new Vector(value));
            for (int i = 0; i < value.getVector().length; i++) {
                newCenter.getVector()[i] += value.getVector()[i];
            }
        }

        for (int i = 0; i < newCenter.getVector().length; i++) {
            newCenter.getVector()[i] = newCenter.getVector()[i] / vectorList.size();
        }

        Vector center = new Vector(newCenter);
        centers.add(center);
        for (Vector vector : vectorList) {
            context.write(center, vector);
        }

//        if (center.converged(key))
//            context.getCounter(Counter.CONVERGED).increment(1);

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();
        Path outPath = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outPath, true);
        try {
            SequenceFile.Writer out = SequenceFile.createWriter(fs,
                    context.getConfiguration(), outPath, Vector.class,
                    IntWritable.class);
            final IntWritable value = new IntWritable(0);
            for (Vector center : centers) {
                out.append(center, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
