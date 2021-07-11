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
import java.io.InputStreamReader;
import java.util.*;

public class TagTextDictionary {
    private static final String SEPARATOR = "  ";
    private TagDictionary tagDic;
    /**
     *  The tag dictionary. *
     */
    private Map<Integer, String> tagText;
    /**
     *  The tag text. *
     */
    private double reducedTextRatio;

    public TagTextDictionary(TagDictionary tagDic, double reducedTextRatio) {
        this.tagText = new HashMap<>();
        this.tagDic = tagDic;
        this.reducedTextRatio = reducedTextRatio;
        load(DatagenParams.tagTextFile);
    }

    /**
     * @param fileName The tag text dictionary file name.
     * @brief Loads the dictionary.
     */
    private void load(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                                         .getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR);
                Integer id = Integer.valueOf(data[0]);
                tagText.put(id, data[1]);
            }
            dictionary.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param id The tag identifier.
     * @return The tag's text.
     * @brief Gets the text associated with the tag.
     */
    public String getTagText(int id) {
        return tagText.get(id);
    }

    /**
     * @param randomTextSize    The random number generator to generate the text's size.
     * @param randomReducedText The random number generator to generate a small text size.
     * @param minSize           The minimum size to generate.
     * @param maxSize           The maximum size to generate.
     * @return
     * @brief Gets a random tag text size.
     */
    public int getRandomTextSize(RNG randomTextSize, RNG randomReducedText, int minSize, int maxSize) {
        if (randomReducedText.nextDouble() > reducedTextRatio) {
            return randomTextSize.nextInt(maxSize - minSize) + minSize;
        }
        return randomTextSize.nextInt((maxSize >> 1) - minSize) + minSize;
    }

    /**
     * @param randomTextSize The random number generator to generate the size.
     * @param minSize        The minimun text size.
     * @param maxSize        The maximum text size.
     * @return
     * @brief Gets a random large text size.
     */
    public int getRandomLargeTextSize(RNG randomTextSize, int minSize, int maxSize) {
        return randomTextSize.nextInt(maxSize - minSize) + minSize;
    }

    /**
     * @param randomTextSize The random number generator to generate the amount of text devoted to each tag.
     * @param tags           The set of tags to generate the text from.
     * @param textSize       The final text size.
     * @return The final text.
     * @brief Generates a text given a set of tags.
     */
    public String generateText(RNG randomTextSize, TreeSet<Integer> tags, int textSize) {
        StringBuilder returnString = new StringBuilder(1024);
        returnString.setLength(0);
        int textSizePerTag = (int) Math.ceil(textSize / (double) tags.size());
        while (returnString.length() < textSize) {
            Iterator<Integer> it = tags.iterator();
            while (it.hasNext() && returnString.length() < textSize) {
                Integer tag = it.next();
                String content = getTagText(tag);
                int thisTagTextSize = Math.min(textSizePerTag, textSize - returnString.length());
                String tagName = tagDic.getName(tag).replace("_", " ");
                tagName = tagName.replace("\"", "\\\"");
                String prefix = "About " + tagName + ", ";
                thisTagTextSize += prefix.length();
                if (thisTagTextSize >= content.length()) {
                    returnString.append(content);
                } else {
                    int startingPos = randomTextSize.nextInt(content.length() - thisTagTextSize + prefix.length());
                    String finalString = content.substring(startingPos, startingPos + thisTagTextSize - prefix
                            .length());
                    returnString.append(prefix);
                    returnString.append(finalString);
                }
            }
        }

        if (returnString.charAt(returnString.length() - 1) != '.') {
            returnString.append(".");
        }
        if (returnString.length() < textSize - 1) {
            returnString.append(" ");
        }
        if (returnString.length() > textSize) {
            returnString.delete(textSize - 1, returnString.length());
            returnString.trimToSize();
        }
        return returnString.toString().replace("|", " ");
    }
}
