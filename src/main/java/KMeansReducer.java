import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;


// calculate a new Vector for these vertices
public class KMeansReducer extends Reducer<ClusterCenter, Vector, ClusterCenter, Vector> {

    private final List<ClusterCenter> centers = new ArrayList<ClusterCenter>();

    @Override
    protected void reduce(ClusterCenter key, Iterable<Vector> values, Context context) throws IOException, InterruptedException {
        ClusterCenter newCenter = new ClusterCenter();
        List<Vector> vectorList = new ArrayList<Vector>();
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

        ClusterCenter center = new ClusterCenter(newCenter);
        centers.add(center);
        for (Vector vector : vectorList) {
            context.write(center, vector);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();

//        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
//                + "org.apache.hadoop.io.serializer.WritableSerialization");

        Path outPath = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outPath, true);
//        throw new IOException();
        SequenceFile.Writer out = SequenceFile.createWriter(conf,  Writer.file(outPath),
                Writer.keyClass(ClusterCenter.class),  Writer.valueClass(IntWritable.class));
        final IntWritable value = new IntWritable(centers.size());
        for (ClusterCenter center : centers) {
            out.append(center, value);
        }
        out.close();
    }
}
