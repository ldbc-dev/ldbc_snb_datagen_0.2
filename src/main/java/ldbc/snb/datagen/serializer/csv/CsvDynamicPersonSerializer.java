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
package ldbc.snb.datagen.serializer.csv;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.util.DateUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ldbc.snb.datagen.serializer.FileName.*;

public class CsvDynamicPersonSerializer extends DynamicPersonSerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(PERSON, PERSON_STUDYAT_UNIVERSITY, PERSON_WORKAT_COMPANY, PERSON_HASINTEREST_TAG, PERSON_KNOWS_PERSON);
    }

    @Override
    public void writeFileHeaders() {
        List<String> dates1 = ImmutableList.of("creationDate", "deletionDate", "explicitlyDeleted");
        // one-to-many edges, single- and multi-valued attributes
        writers.get(PERSON)                     .writeHeader(dates1, ImmutableList.of("id", "firstName", "lastName", "gender", "birthday", "locationIP", "browserUsed", "place", "language", "email"));

        // many-to-many edges
        writers.get(PERSON_KNOWS_PERSON)        .writeHeader(dates1, ImmutableList.of("Person1.id", "Person2.id"));

        List<String> dates2 = ImmutableList.of("creationDate", "deletionDate");
        writers.get(PERSON_HASINTEREST_TAG)     .writeHeader(dates2, ImmutableList.of("Person.id", "Tag.id"));
        writers.get(PERSON_STUDYAT_UNIVERSITY).writeHeader(dates2, ImmutableList.of("Person.id", "Organisation.id", "classYear"));
        writers.get(PERSON_WORKAT_COMPANY).writeHeader(dates2, ImmutableList.of("Person.id", "Organisation.id", "workFrom"));
    }

    public void serialize(final Person person) {
        List<String> dates1 = ImmutableList.of(formatDateTime(person.getCreationDate()), formatDateTime(person.getDeletionDate()), String.valueOf(person.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] id, firstName, lastName, gender, birthday, locationIP, browserUsed, isLocatedIn, language, email
        writers.get(PERSON).writeEntry(dates1, ImmutableList.of(
                Long.toString(person.getAccountId()),
                person.getFirstName(),
                person.getLastName(),
                getGender(person.getGender()),
                formatDate(person.getBirthday()),
                person.getIpAddress().toString(),
                Dictionaries.browsers.getName(person.getBrowserId()),
                Integer.toString(person.getCityId()),
                buildLanguages(person.getLanguages()),
                buildEmail(person.getEmails())
        ));

        List<String> dates2 = ImmutableList.of(formatDateTime(person.getCreationDate()), formatDateTime(person.getDeletionDate()));
        for (Integer interestIdx : person.getInterests()) {
            // creationDate, [deletionDate,] Person.id, Tag.id
            writers.get(PERSON_HASINTEREST_TAG).writeEntry(dates2, ImmutableList.of(
                    Long.toString(person.getAccountId()),
                    Integer.toString(interestIdx)
            ));
        }
    }

    public void serialize(final StudyAt studyAt, final Person person) {
        List<String> dates = ImmutableList.of(formatDateTime(person.getCreationDate()), formatDateTime(person.getDeletionDate()));

        // creationDate, [deletionDate,] Person.id, University.id, classYear
        writers.get(PERSON_STUDYAT_UNIVERSITY).writeEntry(dates, ImmutableList.of(
                Long.toString(studyAt.person),
                Long.toString(studyAt.university),
                DateUtils.formatYear(studyAt.year)
        ));
    }

    public void serialize(final WorkAt workAt, final Person person) {
        List<String> dates = ImmutableList.of(formatDateTime(person.getCreationDate()), formatDateTime(person.getDeletionDate()));

        // creationDate, [deletionDate,] Person.id, Company.id, workFrom
        writers.get(PERSON_WORKAT_COMPANY).writeEntry(dates, ImmutableList.of(
                Long.toString(workAt.person),
                Long.toString(workAt.company),
                DateUtils.formatYear(workAt.year)
        ));
    }

    public void serialize(final Person person, Knows knows) {
        List<String> dates = ImmutableList.of(formatDateTime(knows.getCreationDate()), formatDateTime(knows.getDeletionDate()), String.valueOf(knows.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] Person1.id, Person2.id
        writers.get(PERSON_KNOWS_PERSON).writeEntry(dates, ImmutableList.of(
                Long.toString(person.getAccountId()),
                Long.toString(knows.to().getAccountId())
        ));
    }

    public String getGender(int gender) {
        if (gender == 1) {
            return "male";
        } else {
            return "female";
        }
    }

    public String buildLanguages(List<Integer> languages) {
        return languages.stream()
                .map(l -> Dictionaries.languages.getLanguageName(l))
                .collect(Collectors.joining(";"));
    }

    public String buildEmail(List<String> emails) {
        return Joiner.on(";").join(emails);
    }

    @Override
    protected boolean isDynamic() {
        return true;
    }



//    @Override
//    public Map<FileName, HdfsCsvWriter> initialize(FileSystem fs, String outputDir, int reducerId, boolean isCompressed, boolean dynamic, List<FileName> fileNames) throws IOException {
//        System.out.println("zono0");
//        Map<FileName, HdfsCsvWriter> writers = new HashMap<>();
//
//        for (FileName f : fileNames) {
//            writers.put(f, new HdfsCsvWriter(
//                            fs,
//                            outputDir + "/csv/raw/composite_merge_foreign" + (dynamic ? "/dynamic/" : "/static/") + f.toString() + "/",
//                            String.valueOf(reducerId),
//                            DatagenParams.numUpdateStreams,
//                            isCompressed,
//                            "|"
//                    )
//            );
//        }
//        return writers;
//    }
}
