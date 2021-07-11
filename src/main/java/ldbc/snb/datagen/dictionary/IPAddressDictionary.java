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
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.util.RNG;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class IPAddressDictionary {

    private static final String SEPARATOR_COUNTRY = "   ";
    private static final String SEPARATOR_IP = "[.]";
    private static final String SEPARATOR_MASK = "/";
    private static final int MAX_IP_COUNTRY = 100;
    private TreeMap<Integer, List<IP>> ipsByCountry;
    /**
     *  The country of ips. *
     */
    private PlaceDictionary placeDictionary;

    /**
     *  The location dictionary. *
     */

    public IPAddressDictionary(PlaceDictionary locationDic) {

        this.placeDictionary = locationDic;
        this.ipsByCountry = new TreeMap<>();
        load(DatagenParams.countryAbbrMappingFile, DatagenParams.IPZONE_DIRECTORY);
    }

    /**
     * @param mappingFileName The abbreviations per country.
     * @param baseIPdir       The base directory where ip files are found.
     * @breif Loads dictionary.
     */
    private void load(String mappingFileName, String baseIPdir) {
        String line;
        Map<String, String> countryAbbreMap = new HashMap<>();
        try {
            BufferedReader mappingFile = new BufferedReader(new InputStreamReader(getClass()
                                                                                          .getResourceAsStream(mappingFileName), "UTF-8"));
            while ((line = mappingFile.readLine()) != null) {
                String data[] = line.split(SEPARATOR_COUNTRY);
                String abbr = data[0];
                String countryName = data[1].trim().replace(" ", "_");
                countryAbbreMap.put(countryName, abbr);
            }
            mappingFile.close();

            List<Integer> countries = placeDictionary.getCountries();
            for (int i = 0; i < countries.size(); i++) {
                ipsByCountry.put(countries.get(i), new ArrayList<>());

                //Get the name of file
                String fileName = countryAbbreMap.get(placeDictionary.getPlaceName(countries.get(i)));
                fileName = baseIPdir + "/" + fileName + ".zone";
                BufferedReader ipZoneFile = new BufferedReader(new InputStreamReader(getClass()
                                                                                             .getResourceAsStream(fileName), "UTF-8"));

                int j = 0;
                while ((line = ipZoneFile.readLine()) != null && (j < MAX_IP_COUNTRY)) {
                    String data[] = line.split(SEPARATOR_IP);
                    String maskData[] = data[3].split(SEPARATOR_MASK);
                    int byte1 = Integer.valueOf(data[0]);
                    int byte2 = Integer.valueOf(data[1]);
                    int byte3 = Integer.valueOf(data[2]);
                    int byte4 = Integer.valueOf(maskData[0]);
                    int maskNum = Integer.valueOf(maskData[1]);

                    IP ip = new IP(byte1, byte2, byte3, byte4, maskNum);

                    ipsByCountry.get(i).add(ip);
                    j++;
                }
                ipZoneFile.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public IP getIP(RNG random, int countryId) {
        int finalLocationIndex = countryId;
        while (!placeDictionary.getType(finalLocationIndex).equals(Place.COUNTRY)) {
            finalLocationIndex = placeDictionary.belongsTo(finalLocationIndex);
        }
        List<IP> countryIPs = ipsByCountry.get(finalLocationIndex);
        int idx = random.nextInt(countryIPs.size());

        IP networkIp = countryIPs.get(idx);

        int mask = networkIp.getMask();
        int network = networkIp.getNetwork();

        IP newIp = new IP(network | ((~mask) & random.nextInt()), mask);

        return newIp;
    }

}
