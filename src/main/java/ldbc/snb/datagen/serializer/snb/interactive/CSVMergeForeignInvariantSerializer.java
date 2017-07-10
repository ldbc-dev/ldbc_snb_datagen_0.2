package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.HDFSCSVWriter;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by aprat on 17/02/15.
 */
public class CSVMergeForeignInvariantSerializer extends InvariantSerializer {

    private HDFSCSVWriter[] writers;

    private enum FileNames {
        TAG ("tag"),
        TAG_HAS_TYPE_TAGCLASS("tag_hasType_tagclass"),
        TAGCLASS ("tagclass"),
        TAGCLASS_IS_SUBCLASS_OF_TAGCLASS ("tagclass_isSubclassOf_tagclass"),
        PLACE ("place"),
        ORGANIZATION ("organisation");

        private final String name;

        private FileNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        int numFiles = FileNames.values().length;
        writers = new HDFSCSVWriter[numFiles];
        for( int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"|",conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator",false));
        }

        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add("id");
        arguments.add("name");
        arguments.add("url");
        writers[FileNames.TAG.ordinal()].writeHeader(arguments);

        arguments.clear();
        arguments.add("Tag.id");
        arguments.add("TagClass.id");
        writers[FileNames.TAG_HAS_TYPE_TAGCLASS.ordinal()].writeHeader(arguments);

        arguments.clear();
        arguments.add("id");
        arguments.add("name");
        arguments.add("url");
        writers[FileNames.TAGCLASS.ordinal()].writeHeader(arguments);

        arguments.clear();
        arguments.add("TagClass.id");
        arguments.add("TagClass.id");
        writers[FileNames.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.ordinal()].writeHeader(arguments);

        arguments.clear();
        arguments.add("id");
        arguments.add("name");
        arguments.add("url");
        arguments.add("type");
        arguments.add("isPartOf");
        writers[FileNames.PLACE.ordinal()].writeHeader(arguments);


        arguments.clear();
        arguments.add("id");
        arguments.add("type");
        arguments.add("name");
        arguments.add("url");
        arguments.add("place");
        writers[FileNames.ORGANIZATION.ordinal()].writeHeader(arguments);

    }

    public void close() {
        int numFiles = FileNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    protected void serialize(final Place place) {
        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add(Integer.toString(place.getId()));
        arguments.add(place.getName());
        arguments.add(DBP.getUrl(place.getName()));
        arguments.add(place.getType());

        if (place.getType() == Place.CITY ||
                place.getType() == Place.COUNTRY) {
            arguments.add(Integer.toString(Dictionaries.places.belongsTo(place.getId())));
        } else {
            arguments.add("");
        }
        writers[FileNames.PLACE.ordinal()].writeEntry(arguments);


    }

    protected void serialize(final Organization organization) {
        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add(Long.toString(organization.id));
        arguments.add(organization.type.toString());
        arguments.add(organization.name);
        arguments.add(DBP.getUrl(organization.name));
        arguments.add(Integer.toString(organization.location));
        writers[FileNames.ORGANIZATION.ordinal()].writeEntry(arguments);
    }

    protected void serialize(final TagClass tagClass) {
        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add(Integer.toString(tagClass.id));
        arguments.add(tagClass.name);
        if (tagClass.name.equals("Thing")) {
            arguments.add("http://www.w3.org/2002/07/owl#Thing");
        } else {
            arguments.add(DBPOWL.getUrl(tagClass.name));
        }
        writers[FileNames.TAGCLASS.ordinal()].writeEntry(arguments);

        if (tagClass.parent != -1) {
            arguments.clear();
            arguments.add(Integer.toString(tagClass.id));
            arguments.add(Integer.toString(tagClass.parent));
            writers[FileNames.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS.ordinal()].writeEntry(arguments);
        }
    }

    protected void serialize(final Tag tag) {
        ArrayList<String> arguments = new ArrayList<String>();
        arguments.add(Integer.toString(tag.id));
        arguments.add(tag.name);
        arguments.add(DBP.getUrl(tag.name));
        writers[FileNames.TAG.ordinal()].writeEntry(arguments);

        arguments.clear();
        arguments.add(Integer.toString(tag.id));
        arguments.add(Integer.toString(tag.tagClass));
        writers[FileNames.TAG_HAS_TYPE_TAGCLASS.ordinal()].writeEntry(arguments);
    }

    public void reset() {
        // Intentionally left empty
    }

}
