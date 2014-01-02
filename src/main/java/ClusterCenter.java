import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


public final class ClusterCenter implements WritableComparable<ClusterCenter> {

    private double[] center;

    public ClusterCenter() {
        super();
    }

    public ClusterCenter(double[] center) {
        super();
        int l = center.length;
        this.center = new double[l];
        System.arraycopy(center, 0, this.center, 0, l);
    }

    public double[] getVector() {
        return center;
    }

    public void setVector(double[] center) {
        this.center = center;
    }

    public ClusterCenter(ClusterCenter center) {
        super();
        int l = center.center.length;
        this.center = new double[l];
        System.arraycopy(center.getVector(), 0, this.center, 0, l);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Vector.writeVector(center, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.center = Vector.readVector(in);
    }

    @Override
    public int compareTo(ClusterCenter o) {
        for (int i = 0; i < center.length; i++) {
            double c = center[i] - o.center[i];
            if (c != 0.0d) return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((center == null) ? 0 : Arrays.hashCode(center));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClusterCenter other = (ClusterCenter) obj;
        if (center == null) {
            if (other.center != null)
                return false;
        } else if (!Arrays.equals(center, other.center))
            return false;
        return true;
    }

    @Override
    public final String toString() {
        if (center.length < 50) {
            return Arrays.toString(center);
        } else {
            return center.length + "x1";
        }
    }

    public double measureDistance(double[] set) {
        double sum = 0;
        int length = this.center.length;
        for (int i = 0; i < length; i++) {
            double diff = set[i] - this.center[i];
            sum += (diff * diff);
        }

        return Math.sqrt(sum);
    }
}
