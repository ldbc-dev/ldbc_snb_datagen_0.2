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
import ldbc.snb.datagen.entities.statictype.place.PopularPlace;
import ldbc.snb.datagen.util.RNG;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class PopularPlacesDictionary {

    private PlaceDictionary placeDictionary;
    /**
     *  The location dictionary. *
     */
    private Map<Integer, List<PopularPlace>> popularPlacesByCountry;   /** The popular places by country .**/

    /**
     * @param locationDic The location dictionary.
     * @brief Constructor
     */
    public PopularPlacesDictionary(PlaceDictionary locationDic) {
        this.placeDictionary = locationDic;
        this.popularPlacesByCountry = new HashMap<>();
        for (Integer id : placeDictionary.getCountries()) {
            this.popularPlacesByCountry.put(id, new ArrayList<>());
        }
        load(DatagenParams.popularDictionaryFile);
    }

    /**
     * @param fileName The popular places file name.
     * @brief Loads a popular places file.
     */
    private void load(String fileName) {
        String line;
        String locationName;
        String lastLocationName = "";
        int curLocationId = -1;

        String label;
        try {
            BufferedReader dicPopularPlace = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            while ((line = dicPopularPlace.readLine()) != null) {
                double latt;
                double longt;
                String infos[] = line.split("  ");
                locationName = infos[0];
                if (locationName.compareTo(lastLocationName) != 0) {
                    if (placeDictionary.getCountryId(locationName) != PlaceDictionary.INVALID_LOCATION) {
                        lastLocationName = locationName;
                        curLocationId = placeDictionary.getCountryId(locationName);
                        label = infos[2];
                        latt = Double.parseDouble(infos[3]);
                        longt = Double.parseDouble(infos[4]);
                        popularPlacesByCountry.get(curLocationId).add(new PopularPlace(label, latt, longt));
                    }
                } else {
                    label = infos[2];
                    latt = Double.parseDouble(infos[3]);
                    longt = Double.parseDouble(infos[4]);
                    popularPlacesByCountry.get(curLocationId).add(new PopularPlace(label, latt, longt));
                }
            }
            dicPopularPlace.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param random    The random number generator.
     * @param countryId the locationid
     * @return The popular place identifier.
     * @brief Gets the popular places of a country.
     */
    public short getPopularPlace(RNG random, int countryId) {
        if (popularPlacesByCountry.get(countryId).size() == 0) {
            return -1;
        }
        return (short) random.nextInt(popularPlacesByCountry.get(countryId).size());
    }

    /**
     * @param countryId the id of the country.
     * @param placeId   The popular place id.
     * @return The popular place.
     * @brief Gets a popular place.
     */
    public PopularPlace getPopularPlace(int countryId, int placeId) {
        return popularPlacesByCountry.get(countryId).get(placeId);
    }
}
