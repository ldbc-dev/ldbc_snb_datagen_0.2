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
package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Message;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.entities.dynamic.relations.Like.LikeType;
import ldbc.snb.datagen.generator.tools.PowerDistribution;
import ldbc.snb.datagen.util.Iterators;
import ldbc.snb.datagen.util.RNG;
import ldbc.snb.datagen.util.Streams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class LikeGenerator {

    private final PowerDistribution likesGenerator;
    private Like like;

    LikeGenerator() {
        likesGenerator = new PowerDistribution(1, DatagenParams.maxNumLike, 0.07);
        this.like = new Like();
    }

    public Stream<Like> generateLikes(RNG randomDeleteLike, RNG random, final Forum forum, final Message message, LikeType type) {
        final int numMembers = forum.getMemberships().size();
        final int numLikes = Math.min(likesGenerator.getValue(random), numMembers);
        List<ForumMembership> memberships = forum.getMemberships();
        final int startIndex = numLikes < numMembers ? random.nextInt(numMembers - numLikes) : 0;

        return Streams.stream(Iterators.forIterator(0, i -> i < numLikes, i -> ++i, i -> {
            ForumMembership membership = memberships.get(startIndex + i);

            long minCreationDate = Math.max(membership.getPerson().getCreationDate(), message.getCreationDate()) + DatagenParams.delta;
            long maxCreationDate = Collections.min(Arrays.asList(
                    message.getCreationDate() + DateGenerator.SEVEN_DAYS,
                    membership.getPerson().getDeletionDate(),
                    message.getDeletionDate(),
                    Dictionaries.dates.getSimulationEnd()
            ));
            if (maxCreationDate <= minCreationDate) {
                return Iterators.ForIterator.CONTINUE();
            }
            long likeCreationDate = Dictionaries.dates.randomDate(random, minCreationDate, maxCreationDate);


            long likeDeletionDate;
            boolean isExplicitlyDeleted;
            if (membership.getPerson().isMessageDeleter() && randomDeleteLike.nextDouble() < DatagenParams.probLikeDeleted) {
                isExplicitlyDeleted = true;
                long minDeletionDate = likeCreationDate + DatagenParams.delta;
                long maxDeletionDate = Collections.min(Arrays.asList(
                        membership.getPerson().getDeletionDate(),
                        message.getDeletionDate(),
                        Dictionaries.dates.getSimulationEnd()));
                if (maxDeletionDate <= minDeletionDate) {
                    return Iterators.ForIterator.CONTINUE();
                }
                likeDeletionDate = Dictionaries.dates.powerLawDeleteDate(random, minDeletionDate, maxDeletionDate);
            } else {
                isExplicitlyDeleted = false;
                likeDeletionDate = Collections.min(Arrays.asList(
                        membership.getPerson().getDeletionDate(),
                        message.getDeletionDate()));
            }

            like.setExplicitlyDeleted(isExplicitlyDeleted);
            like.setPerson(membership.getPerson().getAccountId());
            like.setPersonCreationDate(membership.getPerson().getCreationDate());
            like.setMessageId(message.getMessageId());
            like.setCreationDate(likeCreationDate);
            like.setDeletionDate(likeDeletionDate);
            like.setType(type);
            return Iterators.ForIterator.RETURN(like);
        }));
    }
}
