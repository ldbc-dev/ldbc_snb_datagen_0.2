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
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.util.RNG;
import ldbc.snb.datagen.util.ZOrder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * This class reads the files containing the country data and city data used in the LDBC social network generation and
 * provides access methods to the data.
 * Most of the persons has the prerequisite of requiring a valid location id.
 */
public class PlaceDictionary {

    static final int INVALID_LOCATION = -1;
    private static final String SEPARATOR = " ";
    private static final String SEPARATOR_CITY = " ";

    private PlaceZOrder[] sortedPlace;
    private Float[] cumulativeDistribution;

    /**
     * The set of countries. *
     */
    private List<Integer> countries;

    /**
     * The places by id. *
     */
    private Map<Integer, Place> places;

    /**
     * The location hierarchy. *
     */
    private Map<Integer, Integer> isPartOf;

    /**
     * The cities by country. *
     */
    private Map<Integer, List<Integer>> citiesByCountry;

    /**
     * The city names. *
     */
    private Map<String, Integer> cityNames;
    /**
     * The country names. *
     * */
    private Map<String, Integer> countryNames;

    /**
     * Create place dictionary
     */
    public PlaceDictionary() {
        this.countryNames = new HashMap<>();
        this.cityNames = new HashMap<>();
        this.places = new HashMap<>();
        this.isPartOf = new HashMap<>();
        this.countries = new ArrayList<>();
        this.citiesByCountry = new HashMap<>();
        load();
    }

    /**
     * @return The set of places.
     * Gets the set of places.
     */
    public Set<Integer> getPlaces() {
        return places.keySet();
    }

    /**
     * @return The set of countries
     * Gets a list of the country ids.
     */
    public List<Integer> getCountries() {
        return new ArrayList<>(countries);
    }

    /**
     * @param placeId Gets the name of a location.
     * @return The name of the location.
     *  Given a location id returns the name of said place.
     */
    public String getPlaceName(int placeId) {
        return places.get(placeId).getName();
    }


    /**
     * @param placeId The place identifier.
     * @return The type of the place.
     *  Gets the type of a place.
     */
    String getType(int placeId) {
        return places.get(placeId).getType();
    }

    /**
     * @param cityName The name of the city.
     * @return The identifier of the city.
     * Gets The identifier of a city.
     */
    public int getCityId(String cityName) {
        if (!cityNames.containsKey(cityName)) {
            return INVALID_LOCATION;
        }
        return cityNames.get(cityName);
    }

    /**
     * @param countryName The name of the country.
     * @return The identifier ot the country.
     * Gets the identifier of a country.
     */
    public int getCountryId(String countryName) {
        if (!countryNames.containsKey(countryName)) {
            return INVALID_LOCATION;
        }
        return countryNames.get(countryName);
    }

    /**
     * @param placeId The place identifier.
     * @return The parent place identifier.
     * Gets the parent of a place in the place hierarchy.
     */
    public int belongsTo(int placeId) {
        if (!isPartOf.containsKey(placeId)) {
            return INVALID_LOCATION;
        }
        return isPartOf.get(placeId);
    }

    /**
     * @param random    The random  number generator.
     * @param countryId The country Identifier.
     * @return The city identifier.
     * Gets a random city from a country.
     */
    public int getRandomCity(RNG random, int countryId) {
        if (!citiesByCountry.containsKey(countryId)) {
            System.out.println("Invalid countryId");
            return INVALID_LOCATION;
        }
        if (citiesByCountry.get(countryId).size() == 0) {
            Place placeId = places.get(countryId);
            String countryName = placeId.getName();
            System.out.println("Country with no known cities: " + countryName);
            return INVALID_LOCATION;
        }

        int randomNumber = random.nextInt(citiesByCountry.get(countryId).size());
        return citiesByCountry.get(countryId).get(randomNumber);
    }

    /**
     * @param random The random  number generator.
     * @return The country identifier.
     */
    public int getRandomCountryUniform(RNG random) {
        int randomNumber = random.nextInt(countries.size());
        return countries.get(randomNumber);
    }

    /**
     * Loads the dictionary files
     */
    private void load() {

        readCountries(DatagenParams.countryDictionaryFile);
        orderByZ();
        readCities(DatagenParams.cityDictionaryFile);
        readContinents(DatagenParams.countryDictionaryFile);

    }

    /**
     * @param fileName The cities file name to read.
     * Reads a cities file name.
     */
    private void readCities(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String[] data = line.split(SEPARATOR_CITY);
                if (countryNames.containsKey(data[0])) {
                    Integer countryId = countryNames.get(data[0]);
                    if (!cityNames.containsKey(data[1])) {
                        Place placeId = new Place();
                        placeId.setId(places.size());
                        placeId.setName(data[1]);
                        placeId.setLatitude(places.get(countryId).getLatitude());
                        placeId.setLongitude(places.get(countryId).getLongitude());
                        placeId.setPopulation(-1);
                        placeId.setType(Place.CITY);

                        places.put(placeId.getId(), placeId);
                        isPartOf.put(placeId.getId(), countryId);
                        citiesByCountry.get(countryId).add(placeId.getId());

                        cityNames.put(data[1], placeId.getId());
                    }
                }
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param fileName The countries file name.
     * Reads a countries file.
     */
    private void readCountries(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            List<Float> temporalCumulative = new ArrayList<>();

            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String placeName = data[1];

                Place place = new Place();
                place.setId(places.size());
                place.setName(placeName);
                place.setLatitude(Double.parseDouble(data[2]));
                place.setLongitude(Double.parseDouble(data[3]));
                place.setPopulation(Integer.parseInt(data[4]));
                place.setType(Place.COUNTRY);

                places.put(place.getId(), place);
                countryNames.put(placeName, place.getId());
                float dist = Float.parseFloat(data[5]);
                temporalCumulative.add(dist);
                countries.add(place.getId());

                citiesByCountry.put(place.getId(), new ArrayList<>());
            }
            dictionary.close();
            cumulativeDistribution = new Float[temporalCumulative.size()];
            cumulativeDistribution = temporalCumulative.toArray(cumulativeDistribution);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param fileName The continents file name.
     * Reads a continents file name.
     */
    private void readContinents(String fileName) {
        Map<String, Integer> treatedContinents = new HashMap<>();
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String placeName = data[1];

                int countryId = countryNames.get(placeName);

                if (!treatedContinents.containsKey(data[0])) {

                    Place continent = new Place();
                    continent.setId(places.size());
                    continent.setName(data[0]);
                    continent.setLatitude(Double.parseDouble(data[2]));
                    continent.setLongitude(Double.parseDouble(data[3]));
                    continent.setPopulation(0);
                    continent.setType(Place.CONTINENT);

                    places.put(continent.getId(), continent);
                    treatedContinents.put(data[0], continent.getId());
                }
                Integer continentId = treatedContinents.get(data[0]);
                long population = places.get(continentId).getPopulation() + places.get(countryId).getPopulation();
                places.get(continentId).setPopulation(population);
                isPartOf.put(countryId, continentId);
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param random The random number generator.
     * @return The country for the person.
     * Gets a country for a person.
     */
    public int getCountryForPerson(RNG random) {
        int position = Arrays.binarySearch(cumulativeDistribution, random.nextFloat());
        if (position >= 0) {
            return position;
        }
        return (-(position + 1));
    }

    public float getCumProbabilityCountry(int countryId) {
        if (countryId == 0) {
            return cumulativeDistribution[0];
        }
        return cumulativeDistribution[countryId] - cumulativeDistribution[countryId - 1];
    }

    /**
     * Sorts places by Z order.
     */
    private void orderByZ() {
        ZOrder zorder = new ZOrder(8);
        sortedPlace = new PlaceZOrder[countries.size()];

        for (int i = 0; i < countries.size(); i++) {
            Place loc = places.get(countries.get(i));
            int zvalue = zorder.getZValue(((int) Math.round(loc.getLongitude()) + 180) / 2, ((int) Math
                    .round(loc.getLatitude()) + 180) / 2);
            sortedPlace[i] = new PlaceZOrder(loc.getId(), zvalue);
        }

        Arrays.sort(sortedPlace);
        for (int i = 0; i < sortedPlace.length; i++) {
            places.get(sortedPlace[i].id).setZId(i);
        }
    }

    /**
     * @param placeId The place identifier.
     * @return The z order of the place.
     * Gets the z order of a place.
     */
    public int getZorderID(int placeId) {
        return places.get(placeId).getZId();
    }

    /**
     * @param zOrderId the z order.
     * @return The place identifier.
     * Gets the place identifier from a z order.
     */
    public int getPlaceIdFromZOrder(int zOrderId) {
        return sortedPlace[zOrderId].id;
    }

    /**
     * @param id The place identifier.
     * @return The place whose identifier is id.
     * Gets a place from its identifier.
     */
    public Place getLocation(int id) {
        return places.get(id);
    }
}
