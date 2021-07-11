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

import ldbc.snb.datagen.entities.statictype.tag.FlashMobTag;
import ldbc.snb.datagen.generator.generators.DateGenerator;
import ldbc.snb.datagen.generator.tools.PowerDistribution;
import ldbc.snb.datagen.util.RNG;

import java.util.*;

public class FlashmobTagDictionary {

    /**
     *  The date generator used to generate dates.
     */
    private DateGenerator dateGen;

    /**
     *  The powerlaw distribution generator used to generate the levels.
     */
    private PowerDistribution levelGenerator;

    /**
     *  The tag dictionary used to create the flashmob tags.
     */
    private TagDictionary tagDictionary;

    /**
     *  A map of identifiers of tags to flashmob tag instances.
     */
    private Map<Integer, List<FlashMobTag>> flashmobTags;

    /**
     *  The cumulative distribution of flashmob tags sorted by date.
     */
    private FlashMobTag[] flashmobTagCumDist;

    /**
     *  The number of flashmob tags per month.
     */
    private double flashmobTagsPerMonth;

    /**
     *  The probability to take an interest flashmob tag.
     */
    private double probInterestFlashmobTag;

    /**
     *  The probability per level to take a flashmob tag.
     */
    private double probRandomPerLevel;

    public FlashmobTagDictionary(TagDictionary tagDictionary,
                                 DateGenerator dateGen,
                                 int flashmobTagsPerMonth,
                                 double probInterestFlashmobTag,
                                 double probRandomPerLevel,
                                 double flashmobTagMinLevel,
                                 double flashmobTagMaxLevel,
                                 double flashmobTagDistExp) {

        this.tagDictionary = tagDictionary;
        this.dateGen = dateGen;
        this.levelGenerator = new PowerDistribution(flashmobTagMinLevel, flashmobTagMaxLevel, flashmobTagDistExp);
        this.flashmobTags = new HashMap<>();
        this.flashmobTagsPerMonth = flashmobTagsPerMonth;
        this.probInterestFlashmobTag = probInterestFlashmobTag;
        this.probRandomPerLevel = probRandomPerLevel;
        initialize();
    }

    /**
     * Initializes the flashmob tag dictionary, by selecting a set of tags as flashmob tags.
     */
    private void initialize() {
        RNG random = new RNG(0);
        int numFlashmobTags = (int) (flashmobTagsPerMonth * dateGen.numberOfMonths(dateGen.getSimulationStart()));
        Integer[] tags = tagDictionary.getRandomTags(random, numFlashmobTags);
        flashmobTagCumDist = new FlashMobTag[numFlashmobTags];
        double sumLevels = 0;
        for (int i = 0; i < numFlashmobTags; ++i) {
            List<FlashMobTag> instances = flashmobTags.computeIfAbsent(tags[i], k -> new ArrayList<>());
            FlashMobTag flashmobTag = new FlashMobTag();
            flashmobTag.date = dateGen.randomDate(random, dateGen.getSimulationStart());
            flashmobTag.level = levelGenerator.getValue(random);
            sumLevels += flashmobTag.level;
            flashmobTag.tag = tags[i];
            instances.add(flashmobTag);
            flashmobTagCumDist[i] = flashmobTag;
        }
        Arrays.sort(flashmobTagCumDist);
        double currentProb = 0.0;
        for (FlashMobTag flashMobTag : flashmobTagCumDist) {
            flashMobTag.prob = currentProb;
            currentProb += (double) (flashMobTag.level) / sumLevels;
        }
    }

    /**
     * Selects the earliest flashmob tag index from a given date.
     * @param fromDate The minimum date to consider.
     * @return The index to the earliest flashmob tag.
     */
    private int searchEarliestIndex(long fromDate) {
        int lowerBound = 0;
        int upperBound = flashmobTagCumDist.length;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (flashmobTagCumDist[midPoint].date > fromDate) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    /**
     * Makes a decision of selecting or not a flashmob tag.
     * @param rand random number generator
     * @param index The index of the flashmob tag to select.
     * @return true if the flashmob tag is selected. false otherwise.
     */
    private boolean selectFlashmobTag(RNG rand, int index) {
        return rand.nextDouble() > (1 - probRandomPerLevel * flashmobTagCumDist[index].level);
    }


    /**
     * Given a set of interests and a date, generates a set of flashmob tags.
     * @param rand random number generator
     * @param interests The set of interests.
     * @param fromDate The date from which to consider the flashmob tags.
     * @return A vector containing the selected flashmob tags.
     */
    public List<FlashMobTag> generateFlashmobTags(RNG rand, TreeSet<Integer> interests, long fromDate) {
        List<FlashMobTag> result = new ArrayList<>();
        for (Integer tag : interests) {
            List<FlashMobTag> instances = flashmobTags.get(tag);
            if (instances != null) {
                for (FlashMobTag instance : instances) {
                    if ((instance.date >= fromDate) && (rand.nextDouble() > 1 - probInterestFlashmobTag)) {
                        result.add(instance);
                    }
                }
            }
        }
        int earliestIndex = searchEarliestIndex(fromDate);
        for (int i = earliestIndex; i < flashmobTagCumDist.length; ++i) {
            if (selectFlashmobTag(rand, i)) {
                result.add(flashmobTagCumDist[i]);
            }
        }
        return result;
    }

}
