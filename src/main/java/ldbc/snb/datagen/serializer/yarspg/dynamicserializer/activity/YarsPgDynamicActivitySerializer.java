package ldbc.snb.datagen.serializer.yarspg.dynamicserializer.activity;


import avro.shaded.com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.yarspg.EdgeType;
import ldbc.snb.datagen.serializer.yarspg.Relationship;
import ldbc.snb.datagen.serializer.yarspg.Statement;
import ldbc.snb.datagen.serializer.yarspg.YarsPgSerializer;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;
import ldbc.snb.datagen.vocabulary.DC;
import ldbc.snb.datagen.vocabulary.OWL;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static ldbc.snb.datagen.serializer.FileName.*;

public class YarsPgDynamicActivitySerializer extends DynamicActivitySerializer<HdfsYarsPgWriter> implements YarsPgSerializer {
    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(FORUM, FORUM_HASMEMBER_PERSON, FORUM_HASTAG_TAG, PERSON_LIKES_POST,
                PERSON_LIKES_COMMENT, POST, POST_HASTAG_TAG, COMMENT, COMMENT_HASTAG_TAG, FORUM_HASMODERATOR_PERSON,
                FORUM_CONTAINEROF_POST, COMMENT_REPLYOF_POST, COMMENT_REPLYOF_COMMENT);
    }

    @Override
    public void writeFileHeaders() {
        getFileNames().forEach(fileName -> writers.get(fileName).writeHeader(HdfsYarsPgWriter.STANDARD_HEADERS));
    }

    public void serialize(final Forum forum) {
        String forumSchemaEdgeID = Statement.generateId("S_Forum" + forum.getId());
        String forumEdgeID = Statement.generateId("Forum" + forum.getId());
        String schemaNodeID = Statement.generateId("S_Forum" + forum.getId());
        String nodeID = Statement.generateId("Forum" + forum.getId());

        writers.get(FORUM)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Forum")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("title", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("Forum")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(forum.getId()))
                                );
                                properties.add("title", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, forum.getTitle())
                                );
                            });
                });

        for (Integer tagId : forum.getTags()) {
            String tagSchemaEdgeID = Statement.generateId("S_Tag" + tagId);
            String tagEdgeID = Statement.generateId("Tag" + tagId);
            writers.get(FORUM_HASTAG_TAG)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(forumSchemaEdgeID, Relationship.HAS_TAG.toString(), tagSchemaEdgeID);
                        edge.as(forumEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String personSchemaEdgeID = Statement.generateId("S_Person" + forum.getModerator().getAccountId());
        String personEdgeID = Statement.generateId("Person" + forum.getModerator().getAccountId());
        writers.get(FORUM_HASMODERATOR_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(forumSchemaEdgeID, Relationship.HAS_MODERATOR.toString(), personSchemaEdgeID);
                    edge.as(forumEdgeID, Relationship.HAS_MODERATOR.toString(), personEdgeID);
                });
    }

    public void serialize(final Post post) {
        String postSchemaEdgeID = Statement.generateId("S_Post" + post.getMessageId());
        String postEdgeID = Statement.generateId("Post" + post.getMessageId());
        String schemaNodeID = Statement.generateId("S_Post" + post.getMessageId());
        String nodeID = Statement.generateId("Post" + post.getMessageId());

        writers.get(POST)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Post")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("language", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING).asOptional()
                                );
                                propsSchemas.add("imageFile", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING).asOptional()
                                );
                            });


                    node.withNodeLabels("Post")
                            .withProperties(properties -> {
                                properties.add("language", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Dictionaries.languages.getLanguageName(post.getLanguage()))
                                );
                                properties.add("imageFile", property ->
                                        property.generatePrimitive(PrimitiveType.NULL, "null")
                                );
                            });
                });

        for (Integer tagId : post.getTags()) {
            String tagSchemaEdgeID = Statement.generateId("S_Tag" + tagId);
            String tagEdgeID = Statement.generateId("Tag" + tagId);

            writers.get(POST_HASTAG_TAG)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(postSchemaEdgeID, Relationship.HAS_TAG.toString(), tagSchemaEdgeID);
                        edge.as(postEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String forumSchemaEdgeID = Statement.generateId("S_Post" + post.getForumId());
        String forumEdgeID = Statement.generateId("Post" + post.getForumId());
        writers.get(FORUM_CONTAINEROF_POST)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(forumSchemaEdgeID, Relationship.CONTAINER_OF.toString(), postSchemaEdgeID);
                    edge.as(forumEdgeID, Relationship.CONTAINER_OF.toString(), postEdgeID);
                });
    }

    public void serialize(final Comment comment) {
        String commentSchemaEdgeID = Statement.generateId("S_Comment" + comment.getRootPostId());
        String commentEdgeID = Statement.generateId("Comment" + comment.getRootPostId());

        String messageSchemaEdgeID = Statement.generateId("S_Message" + comment.getMessageId());
        String messageEdgeID = Statement.generateId("Message" + comment.getMessageId());

        writers.get(COMMENT_REPLYOF_POST)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(commentSchemaEdgeID, Relationship.IS_REPLY_OF.toString(), messageSchemaEdgeID);
                    edge.as(commentEdgeID, Relationship.IS_REPLY_OF.toString(), messageEdgeID);
                });
    }

    public void serialize(final Photo photo) {
        String postSchemaEdgeID = Statement.generateId("S_Post" + photo.getMessageId());
        String postEdgeID = Statement.generateId("Post" + photo.getMessageId());
        String schemaNodeID = Statement.generateId("S_Post" + photo.getMessageId());
        String nodeID = Statement.generateId("Post" + photo.getMessageId());

        writers.get(POST)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Post")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("language", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING).asOptional()
                                );
                                propsSchemas.add("imageFile", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING).asOptional()
                                );
                            });

                    node.withNodeLabels("Post")
                            .withProperties(properties -> {
                                properties.add("language", property ->
                                        property.generatePrimitive(PrimitiveType.NULL, "null")
                                );
                                properties.add("imageFile", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, photo.getContent())
                                );
                            });
                });

        for (Integer tagId : photo.getTags()) {
            String tagSchemaEdgeID = Statement.generateId("S_Tag" + tagId);
            String tagEdgeID = Statement.generateId("Tag" + tagId);

            writers.get(POST_HASTAG_TAG)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(postSchemaEdgeID, Relationship.HAS_TAG.toString(), tagSchemaEdgeID);
                        edge.as(postEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String forumSchemaEdgeID = Statement.generateId("S_Post" + photo.getForumId());
        String forumEdgeID = Statement.generateId("Post" + photo.getForumId());
        writers.get(FORUM_CONTAINEROF_POST)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(forumSchemaEdgeID, Relationship.CONTAINER_OF.toString(), postSchemaEdgeID);
                    edge.as(forumEdgeID, Relationship.CONTAINER_OF.toString(), postEdgeID);
                });
    }

    public void serialize(final ForumMembership membership) {
        String forumSchemaEdgeID = Statement.generateId("S_Forum" + membership.getForumId());
        String forumEdgeID = Statement.generateId("Forum" + membership.getForumId());

        String personSchemaEdgeID = Statement.generateId("S_Person" + membership.getForumId());
        String personEdgeID = Statement.generateId("Person" + membership.getForumId());

        writers.get(FORUM_HASMEMBER_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(forumSchemaEdgeID, Relationship.HAS_MEMBER.toString(), personSchemaEdgeID)
                            .withPropsDefinition(schemasOfProperty -> schemasOfProperty.add("creationDate",
                                    propertySchema -> propertySchema.generateSchema(PrimitiveType.DATE_TIME)));

                    edge.as(forumEdgeID, Relationship.HAS_MEMBER.toString(), personEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(membership.getCreationDate())));
                            });
                });
    }

    public void serialize(final Like like) {
        String personSchemaEdgeID = Statement.generateId("S_Person" + like.getPerson());
        String personEdgeID = Statement.generateId("Person" + like.getPerson());

        String messageSchemaEdgeID = Statement.generateId("S_Message" + like.getMessageId());
        String messageEdgeID = Statement.generateId("Message" + like.getMessageId());

        HdfsYarsPgWriter writer =
                (like.getType() == Like.LikeType.POST || like.getType() == Like.LikeType.PHOTO)
                        ? writers.get(PERSON_LIKES_POST)
                        : writers.get(PERSON_LIKES_COMMENT);

        writer.writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
            schema.as(personSchemaEdgeID, Relationship.LIKES.toString(), messageSchemaEdgeID)
                    .withPropsDefinition(schemasOfProperty -> schemasOfProperty.add("creationDate",
                            propertySchema -> propertySchema.generateSchema(PrimitiveType.DATE_TIME)));

            edge.as(personEdgeID, Relationship.LIKES.toString(), messageEdgeID)
                    .withProperties(properties -> {
                        properties.add("creationDate", property ->
                                property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(like.getCreationDate())));
                    });
        });
    }

    @Override
    protected boolean isDynamic() {
        return true;
    }

}

