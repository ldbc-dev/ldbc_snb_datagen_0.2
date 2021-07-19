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
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.postgenerators.FlashmobPostGenerator;
import ldbc.snb.datagen.generator.generators.postgenerators.UniformPostGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.LdbcSnbTextGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.util.FactorTable;
import ldbc.snb.datagen.util.Iterators;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.Streams;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class PersonActivityGenerator {

    private long startForumId = 0;

    private RandomGeneratorFarm randomFarm;
    private ForumGenerator forumGenerator;
    private FactorTable factorTable;

    public PersonActivityGenerator() {

        randomFarm = new RandomGeneratorFarm();
        forumGenerator = new ForumGenerator();

        factorTable = new FactorTable();

    }

    private GenActivity generateActivity(Person person, List<Person> block, long blockId) throws AssertionError {
        try {
            factorTable.extractFactors(person);
            return new GenActivity(
                    generateWall(person, blockId)
            );

        } catch (AssertionError e) {
            System.out.println("Assertion error when generating activity!");
            System.out.println(e.getMessage());
            throw e;
        }
    }

    /**
     * Generates the personal wall for a Person. Note, only this Person creates Posts in the wall.
     *
     * @param person Person
     */
    private GenWall<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> generateWall(Person person, long blockId) {

        // Generate wall
        System.out.printf("Start forum id %d\n", startForumId);
        Forum wall = forumGenerator.createWall(randomFarm, startForumId++, person, blockId);

        // Could be null is moderator can't be added
        if (wall == null)
            return new GenWall<>(Stream.empty());

        // creates a forum membership for the moderator
        // only the moderator can post on their wall
        ForumMembership moderator = new ForumMembership(wall.getId(),
                wall.getCreationDate() + DatagenParams.delta,
                wall.getDeletionDate(),
                new PersonSummary(person),
                Forum.ForumType.WALL,
                false);
        // list of members who can post on the wall - only moderator of wall can post on it
        List<ForumMembership> memberships = new ArrayList<>();
        memberships.add(moderator);


        return new GenWall<>(Stream.of(
                new Triplet<>(wall, wall.getMemberships().stream(), Stream.empty()))
        );
    }

    public Stream<GenActivity> generateActivityForBlock(int blockId, List<Person> block) {
        randomFarm.resetRandomGenerators(blockId);
        startForumId = 0;
        return block.stream().map(p -> generateActivity(p, block, blockId));
    }

    public void writeActivityFactors(OutputStream postsWriter, OutputStream tagClassWriter, OutputStream tagWriter, OutputStream firstNameWriter, OutputStream miscWriter) throws IOException {
        factorTable.writeActivityFactors(postsWriter, tagClassWriter, tagWriter, firstNameWriter, miscWriter);
    }

    public void writePersonFactors(OutputStream writer) {
        factorTable.writePersonFactors(writer);
    }

    public FactorTable getFactorTable() {
        return factorTable;
    }
}
