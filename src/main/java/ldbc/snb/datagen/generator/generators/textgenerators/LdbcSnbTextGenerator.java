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
package ldbc.snb.datagen.generator.generators.textgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;
import ldbc.snb.datagen.util.RNG;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public class LdbcSnbTextGenerator extends TextGenerator {

    public LdbcSnbTextGenerator(RNG random, TagDictionary tagDic) {
        super(random, tagDic);
    }

    @Override
    protected void load() {
        // Intentionally left empty
    }

    @Override
    public String generateText(PersonSummary member, TreeSet<Integer> tags, Properties prop) {
        String content = "";
        if (prop.getProperty("type").equals("post")) {

            int textSize;
            if (member.isLargePoster() && this.random.nextDouble() > (1.0f - DatagenParams.ratioLargePost)) {
                textSize = Dictionaries.tagText
                        .getRandomLargeTextSize(this.random, DatagenParams.minLargePostSize, DatagenParams.maxLargePostSize);
                assert textSize <= DatagenParams.maxLargePostSize && textSize >= DatagenParams.minLargePostSize : "Person creation date is larger than membership";
            } else {
                textSize = Dictionaries.tagText
                        .getRandomTextSize(this.random, this.random, DatagenParams.minTextSize, DatagenParams.maxTextSize);
                assert textSize <= DatagenParams.maxTextSize && textSize >= DatagenParams.minTextSize : "Person creation date is larger than membership";
            }
            content = Dictionaries.tagText.generateText(this.random, tags, textSize);
        } else {
            int textSize;
            if (member.isLargePoster() && this.random.nextDouble() > (1.0f - DatagenParams.ratioLargeComment)) {
                textSize = Dictionaries.tagText
                        .getRandomLargeTextSize(this.random, DatagenParams.minLargeCommentSize, DatagenParams.maxLargeCommentSize);
                assert textSize <= DatagenParams.maxLargeCommentSize && textSize >= DatagenParams.minLargeCommentSize : "Person creation date is larger than membership";
            } else {
                textSize = Dictionaries.tagText
                        .getRandomTextSize(this.random, this.random, DatagenParams.minCommentSize, DatagenParams.maxCommentSize);
                assert textSize <= DatagenParams.maxCommentSize && textSize >= DatagenParams.minCommentSize : "Person creation date is larger than membership";
            }
            content = Dictionaries.tagText.generateText(this.random, tags, textSize);
        }
        return content;
    }

}
