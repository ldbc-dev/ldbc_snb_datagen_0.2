/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.util.RNG;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * This class reads the file containing the email domain and its popularity and
 * provides access methods to get such data.
 */
public class EmailDictionary {

    private static final String SEPARATOR = " ";
    private List<String> emails;
    private List<Double> cumulativeDistribution;

    /**
     * @brief Constructor.
     */
    public EmailDictionary() {
        load(DatagenParams.emailDictionaryFile);
    }

    /**
     * @param fileName The dictionary file name to load.
     * @brief Loads the dictionary file.
     */
    private void load(String fileName) {
        try {
            BufferedReader emailDictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            emails = new ArrayList<>();
            cumulativeDistribution = new ArrayList<>();

            String line;
            double cummulativeDist = 0.0;
            while ((line = emailDictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                emails.add(data[0]);
                if (data.length == 2) {
                    cummulativeDist += Double.parseDouble(data[1]);
                    cumulativeDistribution.add(cummulativeDist);
                }
            }
            emailDictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a random email domain based on its popularity.
     */
    public String getRandomEmail(RNG randomTop, RNG randomEmail) {
        int minIdx = 0;
        int maxIdx = cumulativeDistribution.size() - 1;
        double prob = randomTop.nextDouble();
        if (prob > cumulativeDistribution.get(maxIdx)) {
            int Idx = randomEmail.nextInt(emails.size() - cumulativeDistribution.size()) + cumulativeDistribution
                    .size();
            return emails.get(Idx);
        } else if (prob < cumulativeDistribution.get(minIdx)) {
            return emails.get(minIdx);
        }

        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return emails.get(maxIdx);
    }
}
