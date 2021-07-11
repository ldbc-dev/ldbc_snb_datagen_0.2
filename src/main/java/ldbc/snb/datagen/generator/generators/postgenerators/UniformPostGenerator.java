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
package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.util.RNG;

import java.util.Random;


public class UniformPostGenerator extends PostGenerator {

    public UniformPostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        super(generator, commentGenerator, likeGenerator);
    }

    @Override
    protected PostCore generatePostInfo(RNG randomDeletePost, RNG randomTag, RNG randomDate, final Forum forum, final ForumMembership membership, int numComments) {
        PostCore postCore = new PostCore();

        // add creation date
        long minCreationDate = membership.getCreationDate() + DatagenParams.delta;
        long maxCreationDate = Math.min(membership.getDeletionDate(),Dictionaries.dates.getSimulationEnd());
        if (maxCreationDate - minCreationDate < 0) {
            return null;
        }
        long postCreationDate = Dictionaries.dates.randomDate(randomDate, minCreationDate, maxCreationDate);
        postCore.setCreationDate(postCreationDate);

        // add deletion date
        long postDeletionDate;
        if (membership.getPerson().isMessageDeleter() && randomDeletePost.nextDouble() < DatagenParams.postMapping[numComments]) {

            postCore.setExplicitlyDeleted(true);
            long minDeletionDate = postCreationDate + DatagenParams.delta;
            long maxDeletionDate = Math.min(membership.getDeletionDate(), Dictionaries.dates.getSimulationEnd());

            if (maxDeletionDate - minDeletionDate < 0) {
                return null;
            }
            postDeletionDate = Dictionaries.dates.powerLawDeleteDate(randomDate, minDeletionDate, maxDeletionDate);
        } else {
            postCore.setExplicitlyDeleted(false);
            postDeletionDate = Math.min(membership.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
        }

        postCore.setDeletionDate(postDeletionDate);

        // add tags to post
        for (Integer value : forum.getTags()) {
            if (postCore.getTags().isEmpty()) {
                postCore.getTags().add(value);
            } else {
                if (randomTag.nextDouble() < 0.05) {
                    postCore.getTags().add(value);
                }
            }
        }
        return postCore;
    }
}
