package ldbc.snb.datagen.util;

public class RNG {

    private long seed;

    public RNG(long seed) {
        this.seed = seed;
    }

    public long nextLong() {
        seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
        return seed;
    }
    public int nextInt() {
        return (int) nextLong();
    }
    public int nextInt(int bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive");
        } else {
            int r = (int) (this.nextLong() >>> 17);
            int m = bound - 1;
            if ((bound & m) == 0) {
                r = (int)((long)bound * (long)r >> 31);
            } else {
                for(int u = r; u - (r = u % bound) + m < 0; u = (int) (this.nextLong() >>> 17)) {
                }
            }

            return r;
        }
    }
    public double nextDouble() {
        return (((nextLong() >>> 12) << 27) + nextLong() >>> 11) * 0x1.0p-53;
    }
    public float nextFloat() {
        return (float)(this.nextLong() >>> 24) / 1.6777216E7F;
    }
    public void setSeed(long seed) {
        this.seed = seed;
    }
}
