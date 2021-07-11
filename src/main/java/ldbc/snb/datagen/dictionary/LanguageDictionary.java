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
import java.util.*;


public class LanguageDictionary {

    private static final String SEPARATOR = "  ";
    private static final String ISO_ENGLISH_CODE = "en";

    private List<String> languages;
    /**
     *  The array of languages. *
     */
    private Map<Integer, List<Integer>> officalLanguagesByCountry;
    /**
     *  The official languages by country. *
     */
    private Map<Integer, List<Integer>> languagesByCountry;
    /**
     *  The languages by country. *
     */
    private PlaceDictionary placeDictionary;
    /**
     *  The location dictionary. *
     */
    private double probEnglish;
    /**
     *  The probability to speak english. *
     */
    private double probSecondLang;                 /** The probability of speaking a second language. **/

    /**
     * @param locationDic    The location dictionary.
     * @param probEnglish    The probability of speaking english.
     * @param probSecondLang The probability of speaking a second language.
     * @brief Constructor
     */
    public LanguageDictionary(PlaceDictionary locationDic,
                              double probEnglish, double probSecondLang) {
        this.placeDictionary = locationDic;
        this.probEnglish = probEnglish;
        this.probSecondLang = probSecondLang;
        this.languages = new ArrayList<>();
        this.officalLanguagesByCountry = new HashMap<>();
        this.languagesByCountry = new HashMap<>();
        load(DatagenParams.languageDictionaryFile);
    }

    /**
     * @param fileName The name of the dictionary file.
     * @brief Loads a dictionary file.
     */
    private void load(String fileName) {
        try {
            for (Integer id : placeDictionary.getCountries()) {
                officalLanguagesByCountry.put(id, new ArrayList<>());
                languagesByCountry.put(id, new ArrayList<>());
            }
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                                         .getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                if (placeDictionary.getCountryId(data[0]) != PlaceDictionary.INVALID_LOCATION) {
                    for (int i = 1; i < data.length; i++) {
                        Integer countryId = placeDictionary.getCountryId(data[0]);
                        String languageData[] = data[i].split(" ");
                        Integer id = languages.indexOf(languageData[0]);
                        if (id == -1) {
                            id = languages.size();
                            languages.add(languageData[0]);
                        }
                        if (languageData.length == 3) {
                            officalLanguagesByCountry.get(countryId).add(id);
                        }
                        languagesByCountry.get(countryId).add(id);
                    }
                }
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param languageId The language identifier.
     * @return The name of the language.
     * @brief Gets the name of the language.
     */
    public String getLanguageName(int languageId) {
        if (languageId < 0 || languageId >= languages.size()) {
            System.out.println("Trying to acces the invalid language with id=" + languageId);
            return "";
        }
        return languages.get(languageId);
    }

    /**
     * @param random  RNG number generator.
     * @param country The country to retrieve the languages from.
     * @return The set of randomly choosen languages.
     * @breif Gets a set of random languages from a country.
     */
    public List<Integer> getLanguages(RNG random, int country) {
        List<Integer> langSet = new ArrayList<>();
        if (officalLanguagesByCountry.get(country).size() != 0) {
            int id = random.nextInt(officalLanguagesByCountry.get(country).size());
            langSet.add(officalLanguagesByCountry.get(country).get(id));
        } else {
            int id = random.nextInt(languagesByCountry.get(country).size());
            langSet.add(languagesByCountry.get(country).get(id));
        }
        if (random.nextDouble() < probSecondLang) {
            int id = random.nextInt(languagesByCountry.get(country).size());
            if (langSet.indexOf(languagesByCountry.get(country).get(id)) == -1) {
                langSet.add(languagesByCountry.get(country).get(id));
            }
        }
        return langSet;
    }

    /**
     * @param random
     * @return The language.
     * @brief Gets a random language.
     */
    public int getInternationlLanguage(RNG random) {
        Integer languageId = -1;
        if (random.nextDouble() < probEnglish) {
            languageId = languages.indexOf(ISO_ENGLISH_CODE);
        }
        return languageId;
    }
}
