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
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.util.*;
import ldbc.snb.datagen.vocabulary.SN;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.*;
import java.util.stream.Stream;


abstract public class PostGenerator {

    private TextGenerator generator;
    private CommentGenerator commentGenerator;
    private LikeGenerator likeGenerator;

    PostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        this.generator = generator;
        this.commentGenerator = commentGenerator;
        this.likeGenerator = likeGenerator;
    }

    public void initialize() {
        // Intentionally left empty
    }

    public Stream<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> createPosts(RandomGeneratorFarm randomFarm, final Forum forum, final List<ForumMembership> memberships,
                                                                                          long numPostsInForum, Iterator<Long> idIterator, long blockId) {

        Properties properties = new Properties();
        properties.setProperty("type", "post");

        return memberships.stream().flatMap(member -> {
            // generate number of posts by this member
            double numPostsPerMember = numPostsInForum / (double) memberships.size();

            if (numPostsPerMember < 1.0) {
                double prob = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST).nextDouble();
                if (prob < numPostsPerMember) numPostsPerMember = 1.0;
            } else {
                numPostsPerMember = Math.ceil(numPostsPerMember);
            }

            final int numPostsPerMemberInt = (int) numPostsPerMember;
            // 0 to 20
            int numComments = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments + 1);

            return Streams.stream(Iterators.forIterator(0, i -> i < numPostsPerMemberInt, i -> ++i, i -> {
                // create post core
                PostCore postCore = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_POST), randomFarm.get(RandomGeneratorFarm.Aspect.TAG),
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE), forum, member, numComments);

                if (postCore == null)
                    return Iterators.ForIterator.CONTINUE();

                // create content, county, ip - sometimes randomise
                String content = this.generator.generateText(member.getPerson(), postCore.getTags(), properties);
                int country = member.getPerson().getCountry();
                IP ip = member.getPerson().getIpAddress();
                RNG random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
                if (PersonBehavior.changeUsualCountry(random, postCore.getCreationDate())) {
                    random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                    country = Dictionaries.places.getRandomCountryUniform(random);
                    random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                    ip = Dictionaries.ips.getIP(random, country);
                }

                Post post = new Post();

                // create post with above information and from post info
                post.initialize(SN.formId(SN.composeId(idIterator.next(), postCore.getCreationDate()), blockId),
                        postCore.getCreationDate(),
                        postCore.getDeletionDate(),
                        member.getPerson(),
                        forum.getId(),
                        content,
                        new ArrayList<>(postCore.getTags()),
                        country,
                        ip,
                        Dictionaries.browsers.getPostBrowserId(
                                randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),
                                randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),
                                member.getPerson().getBrowserId()),
                        forum.getLanguage(),
                        postCore.isExplicitlyDeleted());

                Stream<Like> likeStream = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1
                    ? likeGenerator.generateLikes(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_LIKES),
                        randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE),
                        forum, post, Like.LikeType.POST)
                        : Stream.empty();


                Stream<Pair<Comment, Stream<Like>>> commentStream = commentGenerator.createComments(randomFarm, forum, post, numComments, idIterator, blockId);

                return Iterators.ForIterator.RETURN(new Triplet<>(post, likeStream, commentStream));
            }));
        });
    }
    protected abstract PostCore generatePostInfo(RNG randomDeletePost, RNG randomTag, RNG randomDate, final Forum forum, final ForumMembership membership, int numComments);
}
