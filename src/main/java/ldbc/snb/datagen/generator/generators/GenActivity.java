package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.stream.Stream;


public class GenActivity {
    public final GenWall<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> genWall;
    public GenActivity(
            GenWall<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> genWall
    ) {
        this.genWall = genWall;
    }
}
