package ldbc.snb.datagen.serializer.csv;


import avro.shaded.com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.FileName;

import java.util.List;

import static ldbc.snb.datagen.serializer.FileName.*;

public class CsvDynamicActivitySerializer extends DynamicActivitySerializer<HdfsCsvWriter> implements CsvSerializer {
    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(FORUM, FORUM_HASMEMBER_PERSON, FORUM_HASTAG_TAG, PERSON_LIKES_POST,
                PERSON_LIKES_COMMENT, POST, POST_HASTAG_TAG, COMMENT, COMMENT_HASTAG_TAG);
    }

    @Override
    public void writeFileHeaders() {
        List<String> dates1 = ImmutableList.of("creationDate", "deletionDate", "explicitlyDeleted");

        List<String> dates2 = ImmutableList.of("creationDate", "deletionDate");

        writers.get(FORUM).writeHeader(dates1, ImmutableList.of("id", "title", "moderator"));
        writers.get(FORUM_HASTAG_TAG).writeHeader(dates2, ImmutableList.of("Forum.id", "Tag.id"));
        writers.get(FORUM_HASMEMBER_PERSON).writeHeader(dates1, ImmutableList.of("Forum.id", "Person.id"));

        writers.get(POST).writeHeader(dates1, ImmutableList.of("id", "imageFile", "locationIP", "browserUsed", "language", "content", "length", "creator", "Forum.id", "place"));
        writers.get(POST_HASTAG_TAG).writeHeader(dates2, ImmutableList.of("Post.id", "Tag.id"));

        writers.get(COMMENT).writeHeader(dates1, ImmutableList.of("id", "locationIP", "browserUsed", "content", "length", "creator", "place", "replyOfPost", "replyOfComment"));
        writers.get(COMMENT_HASTAG_TAG).writeHeader(dates2, ImmutableList.of("Comment.id", "Tag.id"));

        writers.get(PERSON_LIKES_POST).writeHeader(dates1, ImmutableList.of("Person.id", "Post.id"));
        writers.get(PERSON_LIKES_COMMENT).writeHeader(dates2, ImmutableList.of("Person.id", "Comment.id"));

    }

    public void serialize(final Forum forum) {
        List<String> dates = ImmutableList.of(formatDateTime(forum.getCreationDate()), formatDateTime(forum.getDeletionDate()), String.valueOf(forum.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] id, title, moderator
        writers.get(FORUM).writeEntry(dates, ImmutableList.of(
                Long.toString(forum.getId()),
                forum.getTitle(),
                Long.toString(forum.getModerator().getAccountId())
        ));

        List<String> dates2 = ImmutableList.of(formatDateTime(forum.getCreationDate()), formatDateTime(forum.getDeletionDate()));
        for (Integer i : forum.getTags()) {
            // creationDate, [deletionDate,] Forum.id, Tag.id
            writers.get(FORUM_HASTAG_TAG).writeEntry(dates2, ImmutableList.of(
                    Long.toString(forum.getId()),
                    Integer.toString(i)
            ));
        }

    }

    public void serialize(final Post post) {
        List<String> dates1 = ImmutableList.of(formatDateTime(post.getCreationDate()), formatDateTime(post.getDeletionDate()), String.valueOf(post.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] id, imageFile, locationIP, browserUsed, language, content, length, creator, Forum.id, place
        writers.get(POST).writeEntry(dates1, ImmutableList.of(
                Long.toString(post.getMessageId()),
                "",
                post.getIpAddress().toString(),
                Dictionaries.browsers.getName(post.getBrowserId()),
                Dictionaries.languages.getLanguageName(post.getLanguage()),
                post.getContent(),
                Integer.toString(post.getContent().length()),
                Long.toString(post.getAuthor().getAccountId()),
                Long.toString(post.getForumId()),
                Integer.toString(post.getCountryId())
        ));

        List<String> dates2 = ImmutableList.of(formatDateTime(post.getCreationDate()), formatDateTime(post.getDeletionDate()));

        for (Integer t : post.getTags()) {
            // creationDate, [deletionDate,] Post.id, Tag.id
            writers.get(POST_HASTAG_TAG).writeEntry(dates2, ImmutableList.of(
                    Long.toString(post.getMessageId()),
                    Integer.toString(t)
            ));
        }
    }

    public void serialize(final Comment comment) {
        List<String> dates1 = ImmutableList.of(formatDateTime(comment.getCreationDate()), formatDateTime(comment.getDeletionDate()), String.valueOf(comment.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] id, locationIP, browserUsed, content, length, creator, place, parentPost, parentComment
        writers.get(COMMENT).writeEntry(dates1, ImmutableList.of(
                Long.toString(comment.getMessageId()),
                comment.getIpAddress().toString(),
                Dictionaries.browsers.getName(comment.getBrowserId()),
                comment.getContent(),
                Integer.toString(comment.getContent().length()),
                Long.toString(comment.getAuthor().getAccountId()),
                Integer.toString(comment.getCountryId()),
                comment.getParentMessageId() == comment.getRootPostId() ? Long.toString(comment.getRootPostId()) : "",
                comment.getParentMessageId() == comment.getRootPostId() ? "" : Long.toString(comment.getParentMessageId())
        ));

        List<String> dates2 = ImmutableList.of(formatDateTime(comment.getCreationDate()), formatDateTime(comment.getDeletionDate()));
        for (Integer t : comment.getTags()) {
            // creationDate, [deletionDate,] Comment.id, Tag.id
            writers.get(COMMENT_HASTAG_TAG).writeEntry(dates2, ImmutableList.of(
                    Long.toString(comment.getMessageId()),
                    Integer.toString(t)
            ));
        }
    }

    public void serialize(final Photo photo) {
        List<String> dates1 = ImmutableList.of(formatDateTime(photo.getCreationDate()), formatDateTime(photo.getDeletionDate()), String.valueOf(photo.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] id, imageFile, locationIP, browserUsed, language, content, length, creator, Forum.id, place
        writers.get(POST).writeEntry(dates1, ImmutableList.of(
                Long.toString(photo.getMessageId()),
                photo.getContent(),
                photo.getIpAddress().toString(),
                Dictionaries.browsers.getName(photo.getBrowserId()),
                "",
                "",
                Integer.toString(0),
                Long.toString(photo.getAuthor().getAccountId()),
                Long.toString(photo.getForumId()),
                Integer.toString(photo.getCountryId())
        ));

        List<String> dates2 = ImmutableList.of(formatDateTime(photo.getCreationDate()), formatDateTime(photo.getDeletionDate()));
        for (Integer t : photo.getTags()) {
            // creationDate, [deletionDate,] Post.id, Tag.id
            writers.get(POST_HASTAG_TAG).writeEntry(dates2, ImmutableList.of(
                    Long.toString(photo.getMessageId()),
                    Integer.toString(t)
            ));
        }
    }

    public void serialize(final ForumMembership membership) {
        List<String> dates = ImmutableList.of(formatDateTime(membership.getCreationDate()), formatDateTime(membership.getDeletionDate()), String.valueOf(membership.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] Forum.id, Person.id
        writers.get(FORUM_HASMEMBER_PERSON).writeEntry(dates, ImmutableList.of(
                Long.toString(membership.getForumId()),
                Long.toString(membership.getPerson().getAccountId())
        ));
    }

    public void serialize(final Like like) {
        List<String> dates = ImmutableList.of(formatDateTime(like.getCreationDate()), formatDateTime(like.getDeletionDate()), String.valueOf(like.isExplicitlyDeleted()));

        // creationDate, [deletionDate, explicitlyDeleted,] Person.id, Message.id
        List<String> arguments = ImmutableList.of(
                Long.toString(like.getPerson()),
                Long.toString(like.getMessageId())
        );
        if (like.getType() == Like.LikeType.POST || like.getType() == Like.LikeType.PHOTO) {
            writers.get(PERSON_LIKES_POST).writeEntry(dates, arguments);
        } else {
            writers.get(PERSON_LIKES_COMMENT).writeEntry(dates, arguments);
        }
    }


    @Override
    protected boolean isDynamic() {
        return true;
    }

}

