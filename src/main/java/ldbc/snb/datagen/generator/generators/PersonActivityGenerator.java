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
import ldbc.snb.datagen.entities.Pair;
import ldbc.snb.datagen.entities.dynamic.*;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PersonActivityGenerator {

    private long startForumId = 0;
    private Iterator<Long> messageIdIterator;

    private RandomGeneratorFarm randomFarm;
    private ForumGenerator forumGenerator;
    private UniformPostGenerator uniformPostGenerator;
    private FlashmobPostGenerator flashmobPostGenerator;
    private PhotoGenerator photoGenerator;
    private FactorTable factorTable;

    public PersonActivityGenerator() {

        randomFarm = new RandomGeneratorFarm();
        forumGenerator = new ForumGenerator();

        TextGenerator generator = new LdbcSnbTextGenerator(randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT), Dictionaries.tags);
        LikeGenerator likeGenerator = new LikeGenerator();
        CommentGenerator commentGenerator = new CommentGenerator(generator, likeGenerator);
        uniformPostGenerator = new UniformPostGenerator(generator, commentGenerator, likeGenerator);
        flashmobPostGenerator = new FlashmobPostGenerator(generator, commentGenerator, likeGenerator);
        photoGenerator = new PhotoGenerator(likeGenerator);

        factorTable = new FactorTable();

        messageIdIterator = Iterators.numbers(0);
    }

    private Activity generateActivity(Person person, List<Person> block, long blockId) throws AssertionError {
        try {
            factorTable.extractFactors(person);
            return new Activity(
                    generateWall(person, blockId),
                    generateGroups(person, block, blockId).collect(Collectors.toList()),
                    generateAlbums(person, blockId)
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
    private Wall<PostTree> generateWall(Person person, long blockId) {

        // Generate wall
        Forum wall = forumGenerator.createWall(randomFarm, startForumId++, person, blockId);

        // Could be null is moderator can't be added
        if (wall == null)
            return new Wall<>(Collections.emptyList());

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

        Stream<PostTree> uniform = uniformPostGenerator.createPosts(
                randomFarm, wall, memberships,
                numPostsPerGroup(randomFarm, wall, DatagenParams.maxNumPostPerMonth, DatagenParams.maxNumFriends),
                messageIdIterator, blockId);

        Stream<PostTree> flashMob = flashmobPostGenerator.createPosts(
                randomFarm, wall, memberships,
                numPostsPerGroup(randomFarm, wall, DatagenParams.maxNumFlashmobPostPerMonth, DatagenParams.maxNumFriends),
                messageIdIterator, blockId);

        return new Wall<>(Stream.of(
                new WallItem<>(
                        wall,
                        wall.getMemberships(),
                        Stream.concat(uniform, flashMob).collect(Collectors.toList())
                )
        ).collect(Collectors.toList()));
    }

    /**
     * Generates the Groups for a Person. Has 5% chance of becoming a moderator of some group(s).
     *
     * @param person persons
     * @param block  block for persons
     */
    private Stream<Wall<PostTree>> generateGroups(Person person, List<Person> block, long blockId) {

        // generate person created groups
        double moderatorProb = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_MODERATOR).nextDouble();
            int numGroup = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_FORUM)
                    .nextInt(DatagenParams.maxNumGroupCreatedPerPerson) + 1;

        return Streams.stream(Iterators.forIterator(0, i -> i < numGroup, i -> ++i, i -> {
            if (moderatorProb >= DatagenParams.groupModeratorProb)
                return Iterators.ForIterator.CONTINUE();

            Forum group = forumGenerator.createGroup(randomFarm, startForumId++, person, block, blockId);

            Stream<PostTree> uniform = uniformPostGenerator.createPosts(
                    randomFarm,
                    group,
                    group.getMemberships(),
                    numPostsPerGroup(randomFarm, group, DatagenParams.maxNumGroupPostPerMonth, DatagenParams.maxGroupSize),
                    messageIdIterator, blockId);
            Stream<PostTree> flashMob  = flashmobPostGenerator.createPosts(
                    randomFarm,
                    group,
                    group.getMemberships(),
                    numPostsPerGroup(randomFarm, group, DatagenParams.maxNumGroupFlashmobPostPerMonth, DatagenParams.maxGroupSize),
                    messageIdIterator, blockId);

            return Iterators.ForIterator.RETURN(new Wall<>(Stream.of(
                    new WallItem<>(
                            group,
                            group.getMemberships(),
                            Stream.concat(uniform, flashMob).collect(Collectors.toList())
                    )
            ).collect(Collectors.toList())));
        }));
    }

    /**
     * Generates the albums for a Person.
     *
     * @param person person
     */
    private Wall<Pair<Photo, List<Like>>> generateAlbums(Person person, long blockId) {

        // work out number of albums to generate
        int numberOfMonths = (int) Dictionaries.dates.numberOfMonths(person.getCreationDate());
        int numberOfPhotoAlbums = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_PHOTO_ALBUM).nextInt(DatagenParams.maxNumPhotoAlbumsPerMonth + 1);
        int numberOfPhotoAlbumsForMonths = numberOfPhotoAlbums == 0
                ? numberOfPhotoAlbums
                : numberOfMonths * numberOfPhotoAlbums;

        return new Wall<>(Streams.stream(Iterators.forIterator(0, i -> i < numberOfPhotoAlbumsForMonths, i -> ++i, i -> {
            Forum album = forumGenerator.createAlbum(randomFarm, startForumId++, person, i, blockId);
            if (album == null) {
                return Iterators.ForIterator.CONTINUE();
            }

            // number of photos to generate
            int numPhotosInAlbum = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_PHOTO)
                    .nextInt(DatagenParams.maxNumPhotoPerAlbums + 1);
            // create photos

            Stream<Pair<Photo, List<Like>>> photos = photoGenerator.createPhotos(randomFarm, album, numPhotosInAlbum, messageIdIterator, blockId);

            return Iterators.ForIterator.RETURN(new WallItem<>(
                 album, album.getMemberships(), photos.collect(Collectors.toList())
            ));
        })).collect(Collectors.toList()));
    }

    private int numPostsPerGroup(RandomGeneratorFarm randomFarm, Forum forum, int maxPostsPerMonth, int maxMembersPerForum) {
        Random random = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST);
        int numOfMonths = (int) Dictionaries.dates.numberOfMonths(forum.getCreationDate());
        int numberPost;
        if (numOfMonths == 0) {
            numberPost = random.nextInt(maxPostsPerMonth + 1);
        } else {
            numberPost = random.nextInt(maxPostsPerMonth * numOfMonths + 1);
        }
        return (numberPost * forum.getMemberships().size()) / maxMembersPerForum;
    }

    public Stream<Activity> generateActivityForBlock(int blockId, List<Person> block) {
        randomFarm.resetRandomGenerators(blockId);
        startForumId = 0;
        messageIdIterator = Iterators.numbers(0);
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
