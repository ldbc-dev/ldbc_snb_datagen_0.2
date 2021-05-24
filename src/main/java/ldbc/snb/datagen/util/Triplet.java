package ldbc.snb.datagen.util;

import java.util.Objects;

/* Need to have concrete subclasses to avoid java.lang.UnsupportedOperationException:
   Cannot have circular references in bean class, but got the circular reference of
   class class ldbc.snb.datagen.entities.Triplet
 */
public abstract class Triplet<T0, T1, T2> {
    public T0 value0;
    public T1 value1;
    public T2 value2;

    public Triplet(T0 value0, T1 value1, T2 value2) {
        this.value0 = value0;
        this.value1 = value1;
        this.value2 = value2;
    }

    public T0 getValue0() {
        return value0;
    }

    public void setValue0(T0 value0) {
        this.value0 = value0;
    }

    public T1 getValue1() {
        return value1;
    }

    public void setValue1(T1 value1) {
        this.value1 = value1;
    }

    public T2 getValue2() {
        return value2;
    }

    public void setValue2(T2 value2) {
        this.value2 = value2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Triplet)) return false;
        Triplet<?, ?, ?> triplet = (Triplet<?, ?, ?>) o;
        return Objects.equals(value0, triplet.value0) && Objects.equals(value1, triplet.value1) && Objects.equals(value2, triplet.value2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value0, value1, value2);
    }
}
