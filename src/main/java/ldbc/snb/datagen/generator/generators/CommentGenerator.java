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
import ldbc.snb.datagen.entities.dynamic.messages.Message;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.util.*;
import ldbc.snb.datagen.vocabulary.SN;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Stream;

public class CommentGenerator {
    private String[] shortComments = {"ok", "good", "great", "cool", "thx", "fine", "LOL", "roflol", "no way!", "I see", "right", "yes", "no", "duh", "thanks", "maybe"};
    private TextGenerator generator;
    private LikeGenerator likeGenerator;

    CommentGenerator(TextGenerator generator, LikeGenerator likeGenerator) {
        this.generator = generator;
        this.likeGenerator = likeGenerator;
    }

    public Stream<Pair<Comment, Stream<Like>>> createComments(RandomGeneratorFarm randomFarm, final Forum forum, final Post post, long numComments, Iterator<Long> idIterator, long blockId) {

        List<Message> parentCandidates = new ArrayList<>();
        parentCandidates.add(post);

        Properties prop = new Properties();
        prop.setProperty("type", "comment");

        // each iteration adds a new leaf node, for the first iteration this is a child of root Post
        return Streams.stream(Iterators.forIterator(0, i -> i < numComments, i -> ++i, i -> {
            int parentIndex = randomFarm.get(RandomGeneratorFarm.Aspect.REPLY_TO).nextInt(parentCandidates.size()); // pick from parent candidates
            Message parentMessage = parentCandidates.get(parentIndex);
            List<ForumMembership> validMemberships = new ArrayList<>(); // memberships that overlap with the existence of the parent message

            for (ForumMembership membership : forum.getMemberships()) { // parent and membership lifespans overlap

                if ((membership.getCreationDate() < parentMessage.getCreationDate() && membership.getDeletionDate() > parentMessage.getCreationDate()) ||
                        membership.getCreationDate() < parentMessage.getDeletionDate() && membership.getDeletionDate() > parentMessage.getDeletionDate()) {
                    validMemberships.add(membership);
                }

            }

            if (validMemberships.size() == 0) { // skip if no valid membership
                return Iterators.ForIterator.BREAK();
            }

            // get random membership from valid memberships - picking who created the comment
            int membershipIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(validMemberships.size());
            ForumMembership membership = validMemberships.get(membershipIndex);

            TreeSet<Integer> tags = new TreeSet<>();
            String content;

            boolean isShort = false;
            if (randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT).nextDouble() > 0.6666) {

                List<Integer> currentTags = new ArrayList<>();
                for (Integer tag : parentMessage.getTags()) {
                    if (randomFarm.get(RandomGeneratorFarm.Aspect.TAG).nextDouble() > 0.5) {
                        tags.add(tag);
                    }
                    currentTags.add(tag);
                }

                for (int j = 0; j < (int) Math.ceil(parentMessage.getTags().size() / 2.0); ++j) {
                    int randomTag = currentTags.get(randomFarm.get(RandomGeneratorFarm.Aspect.TAG)
                            .nextInt(currentTags.size()));
                    tags.add(Dictionaries.tagMatrix.getRandomRelated(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomTag));
                }
                content = this.generator.generateText(membership.getPerson(), tags, prop);
            } else {
                isShort = true;
                int index = randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE).nextInt(shortComments.length);
                content = shortComments[index];
            }

            // creation date
            long minCreationDate = Math.max(parentMessage.getCreationDate(), membership.getCreationDate()) + DatagenParams.delta;
            long maxCreationDate = Collections.min(Arrays.asList(membership.getDeletionDate(), parentMessage.getDeletionDate(), Dictionaries.dates.getSimulationEnd()));
            if (maxCreationDate <= minCreationDate) {
                return Iterators.ForIterator.CONTINUE();
            }
            // powerlaw distribtion
            long creationDate = Dictionaries.dates.powerLawCommDateDay(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), minCreationDate);
            if (creationDate > maxCreationDate) {
                return Iterators.ForIterator.CONTINUE();
            }

            long deletionDate;
            boolean isExplicitlyDeleted;

            // if person is a deleter and selected for delete
            if (membership.getPerson().isMessageDeleter() && randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_COMM).nextDouble() < DatagenParams.probCommentDeleted) {
                isExplicitlyDeleted = true;
                long minDeletionDate = creationDate + DatagenParams.delta;
                long maxDeletionDate = Collections.min(Arrays.asList(parentMessage.getDeletionDate(), membership.getDeletionDate(), Dictionaries.dates.getSimulationEnd()));
                if (maxDeletionDate <= minDeletionDate) {
                    return Iterators.ForIterator.CONTINUE();
                }
                deletionDate = Dictionaries.dates.powerLawDeleteDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), minDeletionDate, maxDeletionDate);
            } else {
                isExplicitlyDeleted = false;
                deletionDate = Collections.min(Arrays.asList(parentMessage.getDeletionDate(), membership.getDeletionDate()));
            }

            int country = membership.getPerson().getCountry();
            IP ip = membership.getPerson().getIpAddress();
            RNG random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
            if (PersonBehavior.changeUsualCountry(random, creationDate)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            Comment comment = new Comment(SN.formId(SN.composeId(idIterator.next(), creationDate), blockId),
                    creationDate,
                    deletionDate,
                    membership.getPerson(),
                    forum.getId(),
                    content,
                    new ArrayList<>(tags),
                    country,
                    ip,
                    Dictionaries.browsers.getPostBrowserId(randomFarm
                            .get(RandomGeneratorFarm.Aspect.DIFF_BROWSER), randomFarm
                            .get(RandomGeneratorFarm.Aspect.BROWSER), membership
                            .getPerson().getBrowserId()),
                    post.getMessageId(),
                    parentMessage.getMessageId(),
                    isExplicitlyDeleted);
            if (!isShort) parentCandidates.add(new Comment(comment));

            Stream<Like> likeStream = comment.getContent().length() > 10
                    && randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1
                    ? likeGenerator.generateLikes(
                            randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_LIKES),
                    randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE), forum, comment, Like.LikeType.COMMENT)
                    : Stream.empty();

            return Iterators.ForIterator.RETURN(new Pair<>(comment, likeStream));
        }));
    }

}
