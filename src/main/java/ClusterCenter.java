import org.apache.hadoop.io.Text;

/**
 * User: dv
 * Date: 2/1/14
 * Time: 8:23 PM
 */
public class ClusterCenter {

    private Text name;
    private Vector center;

    public ClusterCenter(Text name, Vector center) {
        this.name = name;
        this.center = center;
    }

    public Text getName() {
        return name;
    }

    public Vector getCenter() {
        return center;
    }

    public double measureDistance(double[] set) {
        double sum = 0;
        double[] vector = this.center.getVector();
        int length = vector.length;
        for (int i = 0; i < length; i++)
            sum += (set[i] - vector[i]);

        return Math.sqrt(sum);
    }
}
