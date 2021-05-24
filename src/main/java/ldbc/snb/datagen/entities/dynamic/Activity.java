package ldbc.snb.datagen.entities.dynamic;

import ldbc.snb.datagen.entities.Pair;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.relations.Like;

import java.io.Serializable;
import java.util.List;

public class Activity implements Serializable {
    public Wall<PostTree> wall;
    public List<Wall<PostTree>> groups;
    public Wall<Pair<Photo, List<Like>>> albums;

    public Activity(
            Wall<PostTree> wall,
            List<Wall<PostTree>> groups,
            Wall<Pair<Photo, List<Like>>> albums
    ) {
        this.wall = wall;
        this.groups = groups;
        this.albums = albums;
    }

    public Wall<PostTree> getWall() {
        return wall;
    }

    public void setWall(Wall<PostTree> wall) {
        this.wall = wall;
    }

    public List<Wall<PostTree>> getGroups() {
        return groups;
    }

    public void setGroups(List<Wall<PostTree>> groups) {
        this.groups = groups;
    }

    public Wall<Pair<Photo, List<Like>>> getAlbums() {
        return albums;
    }

    public void setAlbums(Wall<Pair<Photo, List<Like>>> albums) {
        this.albums = albums;
    }
}
