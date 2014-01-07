import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text, Vector, Text, Vector> {

    private final List<ClusterCenter> clusterCenters = new ArrayList<ClusterCenter>();

    @Override
    protected void reduce(Text key, Iterable<Vector> values, Context context) throws IOException, InterruptedException {
        Vector newCenter = new Vector();
        List<Vector> vectorList = new ArrayList<Vector>();
        newCenter.setVector(new double[5]);
        for (Vector value : values) {
            vectorList.add(new Vector(value));
            for (int i = 0; i < value.getVector().length; i++) {
                newCenter.getVector()[i] += value.getVector()[i];
            }
        }

        for (int i = 0; i < newCenter.getVector().length; i++) {
            newCenter.getVector()[i] = newCenter.getVector()[i] / vectorList.size();
        }

        ClusterCenter center = new ClusterCenter(new Text(key), newCenter);
        clusterCenters.add(center);
        for (Vector vector : vectorList) {
            context.write(key, vector);
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();

        Path outPath = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outPath, true);

        SequenceFile.Writer out = SequenceFile.createWriter(conf,  Writer.file(outPath),
                Writer.keyClass(Text.class),  Writer.valueClass(Vector.class));
        for (ClusterCenter clusterCenter : clusterCenters) {
            out.append(clusterCenter.getName(), clusterCenter.getCenter());
        }
        out.close();
    }
}
