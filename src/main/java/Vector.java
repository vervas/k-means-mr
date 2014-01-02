import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;


public final class Vector implements Writable {

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

    @Override
    public final void write(DataOutput out) throws IOException {
        out.writeInt(vector.length);
        for (double vector : this.vector) out.writeDouble(vector);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int size = in.readInt();
        vector = new double[size];
        for (int i = 0; i < size; i++) vector[i] = in.readDouble();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((vector == null) ? 0 : Arrays.hashCode(vector));
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
        Vector other = (Vector) obj;
        if (vector == null) {
            if (other.vector != null)
                return false;
        } else if (!Arrays.equals(vector, other.vector))
            return false;
        return true;
    }

    @Override
    public final String toString() {
        if (vector.length < 50) {
            return Arrays.toString(vector);
        } else {
            return vector.length + "x1";
        }
    }

    public double measureDistance(double[] set) {
        double sum = 0;
        int length = this.vector.length;
        for (int i = 0; i < length; i++) {
            double diff = set[i] - this.vector[i];
            sum += (diff * diff);
        }

        return Math.sqrt(sum);
    }
}
