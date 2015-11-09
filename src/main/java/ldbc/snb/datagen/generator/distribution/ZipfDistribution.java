package ldbc.snb.datagen.generator.distribution;


import org.apache.hadoop.conf.Configuration;



/**
 * Created by aprat on 5/03/15.
 */
public class ZipfDistribution implements DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf_;
    private double ALPHA_ = 1.7;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.ZipfDistribution.alpha",ALPHA_);
        zipf_ = new org.apache.commons.math3.distribution.ZipfDistribution(10000, ALPHA_);
    }

    public void reset (long seed) {
        zipf_.reseedRandomGenerator(seed);
    }

    public long nextDegree(){
        return zipf_.sample();
    }
}
