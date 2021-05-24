package ldbc.snb.datagen.entities;

import java.util.Objects;

final public class Pair<T0, T1> {
    public T0 value0;
    public T1 value1;

    public Pair(T0 value0, T1 value1) {
        this.value0 = value0;
        this.value1 = value1;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(value0, pair.value0) && Objects.equals(value1, pair.value1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value0, value1);
    }
}