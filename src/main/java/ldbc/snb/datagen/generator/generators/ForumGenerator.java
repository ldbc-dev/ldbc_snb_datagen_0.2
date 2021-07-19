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
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.StringUtils;
import ldbc.snb.datagen.vocabulary.SN;

import java.util.*;

/**
 * This class generates Forums (Walls, Groups and Albums).
 */
public class ForumGenerator {

    /**
     * Creates a personal wall for a given Person. All friends become members
     *
     * @param randomFarm randomFarm
     * @param forumId    forumID
     * @param person     Person
     * @return Forum
     */
    Forum createWall(RandomGeneratorFarm randomFarm, long forumId, Person person, long blockId) {

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());

        // Check moderator can be added
        if (person.getDeletionDate() - person.getCreationDate() + DatagenParams.delta < 0){
            // what to return?
            return null;
        }

        System.out.printf("Composing Forum (Wall) id for person id = %d, creationDate = %d. The blockId is %d\n", person.getAccountId(), person.getCreationDate(), blockId);
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, person.getCreationDate() + DatagenParams.delta), blockId),
                person.getCreationDate() + DatagenParams.delta,
                person.getDeletionDate(),
                new PersonSummary(person),
                person.getDeletionDate(),
                StringUtils.clampString("Wall of " + person.getFirstName() + " " + person.getLastName(), 256),
                person.getCityId(),
                language,
                Forum.ForumType.WALL,
                false);

        // wall inherits tags from person
        List<Integer> forumTags = new ArrayList<>(person.getInterests());
        forum.setTags(forumTags);

        // adds all friends as members of wall
        List<Knows> knows = person.getKnows();

        // for each friend generate hasMember edge
        for (Knows know : knows) {
            long hasMemberCreationDate = know.getCreationDate() + DatagenParams.delta;
            long hasMemberDeletionDate = Math.min(forum.getDeletionDate(), know.getDeletionDate());
            if (hasMemberDeletionDate - hasMemberCreationDate < 0){
                continue;
            }
            forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, know.to(), Forum.ForumType.WALL, false));
        }
        return forum;
    }

}
