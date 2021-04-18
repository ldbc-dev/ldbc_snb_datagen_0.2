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
package ldbc.snb.datagen.serializer;

import com.google.common.base.Joiner;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;


abstract public class DynamicPersonSerializer<TWriter extends HdfsWriter> extends LdbcSerializer<TWriter> {

    abstract protected void serialize(final Person p);

    abstract protected void serialize(final StudyAt studyAt, final Person person);

    abstract protected void serialize(final WorkAt workAt, final Person person);

    abstract protected void serialize(final Person p, final Knows knows);

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

    public String buildEmail(TreeSet<String> emails) {
        return Joiner.on(";").join(emails);
    }

    @Override
    protected boolean isDynamic() {
        return true;
    }

}