package ldbc.snb.datagen.entities.dynamic;

import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.util.Triplet;

import java.util.List;

public class WallItem<T> extends Triplet<Forum, List<ForumMembership>, List<T>> {
    public WallItem(Forum value0, List<ForumMembership> value1, List<T> value2) {
        super(value0, value1, value2);
    }
}
