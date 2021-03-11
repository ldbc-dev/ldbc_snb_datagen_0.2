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

import java.util.List;

import static ldbc.snb.datagen.serializer.FileName.SOCIAL_NETWORK_ACTIVITY;

public class YarsPgDynamicActivitySerializer extends DynamicActivitySerializer<HdfsYarsPgWriter> implements YarsPgSerializer {
    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_ACTIVITY);
    }

    @Override
    public void writeFileHeaders() {
    }

    public void serialize(final Forum forum) {
        String forumSchemaEdgeID = Statement.generateId("S_Forum" + forum.getId());
        String forumEdgeID = Statement.generateId("Forum" + forum.getId());
        String schemaNodeID = Statement.generateId("S_Forum" + forum.getId());
        String nodeID = Statement.generateId("Forum" + forum.getId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
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
            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(forumSchemaEdgeID, Relationship.HAS_TAG.toString(), tagSchemaEdgeID);
                        edge.as(forumEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String personSchemaEdgeID = Statement.generateId("S_Person" + forum.getModerator().getAccountId());
        String personEdgeID = Statement.generateId("Person" + forum.getModerator().getAccountId());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
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

        writers.get(SOCIAL_NETWORK_ACTIVITY)
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

            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(postSchemaEdgeID, Relationship.HAS_TAG.toString(), tagSchemaEdgeID);
                        edge.as(postEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String forumSchemaEdgeID = Statement.generateId("S_Post" + post.getForumId());
        String forumEdgeID = Statement.generateId("Post" + post.getForumId());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
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

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(commentSchemaEdgeID, Relationship.IS_REPLY_OF.toString(), messageSchemaEdgeID);
                    edge.as(commentEdgeID, Relationship.IS_REPLY_OF.toString(), messageEdgeID);
                });
    }

    public void serialize(final Photo photo) {
    }

    public void serialize(final ForumMembership membership) {
        String forumSchemaEdgeID = Statement.generateId("S_Forum" + membership.getForumId());
        String forumEdgeID = Statement.generateId("Forum" + membership.getForumId());

        String personSchemaEdgeID = Statement.generateId("S_Person" + membership.getForumId());
        String personEdgeID = Statement.generateId("Person" + membership.getForumId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
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

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
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
