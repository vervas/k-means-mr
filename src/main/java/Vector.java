import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public final class Vector implements WritableComparable<Vector> {

    private double[] vector;

    public Vector() {
        super();
    }

    public Vector(double[] vector) {
        super();
        int l = vector.length;
        this.vector = new double[l];
        System.arraycopy(vector, 0, this.vector, 0, l);
    }

    public double[] getVector() {
        return vector;
    }

    public void setVector(double[] vector) {
        this.vector = vector;
    }

    public Vector(Vector v) {
        super();
        int l = v.vector.length;
        this.vector = new double[l];
        System.arraycopy(v.getVector(), 0, this.vector, 0, l);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(vector.length);
        for (double aVector : vector) out.writeDouble(aVector);
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        vector = new double[size];
        for (int i = 0; i < size; i++)
            vector[i] = in.readDouble();
    }

    public int compareTo(Vector o) {
        for (int i = 0; i < vector.length; i++) {
            double c = vector[i] - o.vector[i];
            if (c != 0.0d) return 1;
        }
        return 0;
    }
}
