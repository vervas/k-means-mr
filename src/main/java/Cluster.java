import org.apache.hadoop.io.Text;

/**
 * User: dv
 * Date: 2/1/14
 * Time: 8:23 PM
 */
public class Cluster {

    Text name;
    private Vector center;

    public Cluster(){}

    public Cluster (Text name, Vector center) {
        this.name = name;
        this.center = center;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public Vector getCenter() {
        return center;
    }

    public void setCenter(Vector center) {
        this.center = center;
    }
}
