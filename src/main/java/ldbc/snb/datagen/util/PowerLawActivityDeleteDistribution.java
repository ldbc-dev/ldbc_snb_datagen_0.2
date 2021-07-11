package ldbc.snb.datagen.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PowerLawActivityDeleteDistribution {

    private double[] minutes;
    private double[] distribution;
    private String distributionFile;

    public PowerLawActivityDeleteDistribution(String distributionFile) {
        this.distributionFile = distributionFile;

        this.minutes = new double[]{0, 0.5, 1, 5, 10, 20, 30, 40, 60, 120, 300, 1440, 2880, 4320, 5760, 7200, 8460, 10080};

    }

    public void initialize() {
        try {
            BufferedReader distributionBuffer = new BufferedReader(new InputStreamReader(getClass()
                    .getResourceAsStream(distributionFile), StandardCharsets.UTF_8));
            List<Double> temp = new ArrayList<>();
            String line;
            while ((line = distributionBuffer.readLine()) != null) {
                Double prob = Double.valueOf(line);
                temp.add(prob);
            }
            distribution = new double[temp.size()];
            int index = 0;
            for (Double aDouble : temp) {
                distribution[index] = aDouble;
                ++index;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public double nextDouble(double prob, RNG random) {

        double draw = 0;
        for (int i = 0; i < distribution.length; i++) {
            if (prob < distribution[i]) {
                double lower = minutes[i-1];
                double upper = minutes[i];
                draw = lower + (upper - lower) * random.nextDouble();
                break;
            }
        }
        return draw;

    }



}
