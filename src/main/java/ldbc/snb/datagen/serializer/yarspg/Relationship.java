package ldbc.snb.datagen.serializer.yarspg;

public enum Relationship {

    KNOWS("knows"),
    WORK_AT("workAt"),
    STUDY_AT("studyAt"),
    IS_LOCATED_IN("isLocatedIn"),
    HAS_INTEREST("hasInterest"),
    SPEAKS_LANGUAGE("speaksLanguage"),
    HAS_TAG("hasTag"),
    CONTAINER_OF("containerOf"),
    HAS_MEMBER("hasMember"),
    HAS_MODERATOR("hasModerator"),
    IS_REPLY_OF("isReplyOf"),
    LIKES("likes"),
    IS_SUBCLASS_OF("isSubclassOf"),
    HAS_TYPE("hasType"),
    HAS_CREATOR("hasCreator"),
    IS_PART_OF("isPartOf"),
    ;

    private final String name;

    Relationship(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}
