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
package ldbc.snb.datagen.hadoop.serializer;

import ldbc.snb.datagen.DatagenContext;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.csv.CsvStaticSerializer;
import ldbc.snb.datagen.serializer.yarspg.staticserializer.YarsPgCanonicalSchemalessStaticSerializer;
import ldbc.snb.datagen.serializer.yarspg.staticserializer.YarsPgCanonicalStaticSerializer;
import ldbc.snb.datagen.serializer.yarspg.staticserializer.YarsPgSchemalessStaticSerializer;
import ldbc.snb.datagen.serializer.yarspg.staticserializer.YarsPgStaticSerializer;
import ldbc.snb.datagen.util.GeneratorConfiguration;
import ldbc.snb.datagen.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class HadoopStaticSerializer {

    private final int numPartitions;
    private StaticSerializer[] staticSerializer;
    private TreeSet<Integer> exportedClasses;
    private int currentFile = 0;

    private Configuration hadoopConf;
    private GeneratorConfiguration conf;

    public HadoopStaticSerializer(GeneratorConfiguration conf, Configuration hadoopConf, int numPartitions) {
        this.conf = conf;
        this.hadoopConf = hadoopConf;
        this.numPartitions = numPartitions;
        exportedClasses = new TreeSet<>();
        DatagenContext.initialize(conf);
    }

    public void run() {
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            staticSerializer = new StaticSerializer[numPartitions];
            for (int i = 0; i < numPartitions; ++i) {
                String format = conf.get("serializer.format");
                switch ((format != null) ? format : "CsvBasic") {
                    case "YarsPG":
                        staticSerializer[i] = new YarsPgStaticSerializer();
                        break;
                    case "YarsPGSchemaless":
                        staticSerializer[i] = new YarsPgSchemalessStaticSerializer();
                        break;
                    case "YarsPGCanonical":
                        staticSerializer[i] = new YarsPgCanonicalStaticSerializer();
                        break;
                    case "YarsPGCanonicalSchemaless":
                        staticSerializer[i] = new YarsPgCanonicalSchemalessStaticSerializer();
                        break;
                    case "CsvBasic":
                    default:
                        staticSerializer[i] = new CsvStaticSerializer();
                }

                staticSerializer[i].initialize(
                        fs, conf.getOutputDir(), i, 1.0,
                        false
                );
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }

        exportPlaces();
        exportTags();
        exportOrganisations();

        for (int i = 0; i < numPartitions; ++i) {
            staticSerializer[i].close();
        }
    }

    private int nextFile() {
        int ret = currentFile;
        currentFile = (++currentFile) % numPartitions;
        return ret;
    }

    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses.contains(classId)) {
            exportedClasses.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256);
            tagClass.parent = Dictionaries.tags.getClassParent(tagClass.id);
            staticSerializer[nextFile()].serialize(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = Dictionaries.places.getPlaces();
        for (Integer location : locations) {
            Place place = Dictionaries.places.getLocation(location);
            place.setName(StringUtils.clampString(place.getName(), 256));
            staticSerializer[nextFile()].serialize(place);
        }
    }

    public void exportOrganisations() {
        Set<Long> companies = Dictionaries.companies.getCompanies();
        Iterator<Long> it = companies.iterator();
        while (it.hasNext()) {
            Organisation company = new Organisation();
            company.id = it.next();
            company.type = Organisation.OrganisationType.Company;
            company.name = StringUtils.clampString(Dictionaries.companies.getCompanyName(company.id), 256);
            company.location = Dictionaries.companies.getCountry(company.id);
            staticSerializer[nextFile()].serialize(company);
        }

        Set<Long> universities = Dictionaries.universities.getUniversities();
        it = universities.iterator();
        while (it.hasNext()) {
            Organisation university = new Organisation();
            university.id = it.next();
            university.type = Organisation.OrganisationType.University;
            university.name = StringUtils.clampString(Dictionaries.universities.getUniversityName(university.id), 256);
            university.location = Dictionaries.universities.getUniversityCity(university.id);
            staticSerializer[nextFile()].serialize(university);
        }
    }

    public void exportTags() {
        Set<Integer> tags = Dictionaries.tags.getTags();
        for (Integer integer : tags) {
            Tag tag = new Tag();
            tag.id = integer;
            tag.name = StringUtils.clampString(Dictionaries.tags.getName(tag.id), 256);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = Dictionaries.tags.getTagClass(tag.id);
            staticSerializer[nextFile()].serialize(tag);
            exportTagHierarchy(tag);
        }
    }
}
