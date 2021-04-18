package ldbc.snb.datagen.serializer.yarspg.dynamicserializer.person;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.yarspg.EdgeType;
import ldbc.snb.datagen.serializer.yarspg.Relationship;
import ldbc.snb.datagen.serializer.yarspg.Statement;
import ldbc.snb.datagen.serializer.yarspg.YarsPgSerializer;
import ldbc.snb.datagen.serializer.yarspg.property.ComplexType;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;
import ldbc.snb.datagen.util.DateUtils;

import java.util.List;
import java.util.stream.Collectors;

import static ldbc.snb.datagen.serializer.FileName.*;

public class YarsPgDynamicPersonSerializer extends DynamicPersonSerializer<HdfsYarsPgWriter> implements YarsPgSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(PERSON, PERSON_STUDYAT_UNIVERSITY, PERSON_WORKAT_COMPANY, PERSON_HASINTEREST_TAG,
                PERSON_ISLOCATEDIN_CITY, PERSON_KNOWS_PERSON);
    }

    @Override
    public void writeFileHeaders() {
        getFileNames().forEach(fileName -> writers.get(fileName).writeHeader(HdfsYarsPgWriter.STANDARD_HEADERS));
    }

    @Override
    protected void serialize(final Person person) {
        String schemaNodeID = Statement.generateId("S_Person" + person.getAccountId());
        String nodeID = Statement.generateId("Person" + person.getAccountId());

        writers.get(PERSON)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Person")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("firstName", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("lastName", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("gender", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("birthday", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.DATE)
                                );
                                propsSchemas.add("email", propSchema ->
                                        propSchema.generateSchema(1, ComplexType.LIST, PrimitiveType.STRING)
                                );
                                propsSchemas.add("speaks", propSchema ->
                                        propSchema.generateSchema(1, ComplexType.LIST, PrimitiveType.STRING)
                                );
                                propsSchemas.add("browserUsed", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("locationIP", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("Person")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(person.getAccountId()))
                                );
                                properties.add("firstName", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, person.getFirstName())
                                );
                                properties.add("lastName", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, person.getLastName())
                                );
                                properties.add("gender", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, getGender(person.getGender()))
                                );
                                properties.add("birthday", property ->
                                        property.generatePrimitive(PrimitiveType.DATE,
                                                formatDate(person.getBirthday()))
                                );
                                properties.add("email", property ->
                                        property.generateComplex(ComplexType.LIST,
                                                PrimitiveType.STRING,
                                                person.getEmails())
                                );

                                properties.add("speaks", property ->
                                        property.generateComplex(
                                                ComplexType.LIST, PrimitiveType.STRING,
                                                person.getLanguages()
                                                        .stream()
                                                        .map(l -> Dictionaries.languages.getLanguageName(l))
                                                        .collect(
                                                                Collectors.toList()))
                                );

                                properties.add("browserUsed", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Dictionaries.browsers.getName(
                                                        person.getBrowserId()))
                                );
                                properties.add("locationIP", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, person.getIpAddress()
                                                .toString())
                                );
                            });
                });

        String personSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String personEdgeID = Statement.generateId("Person" + person.getAccountId());
        String citySchemaEdgeID = Statement.generateId("S_City" + person.getCityId());
        String cityEdgeID = Statement.generateId("City" + person.getCityId());

        writers.get(PERSON_ISLOCATEDIN_CITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(personSchemaEdgeID, Relationship.IS_LOCATED_IN.toString(), citySchemaEdgeID);
                    edge.as(personEdgeID, Relationship.IS_LOCATED_IN.toString(), cityEdgeID);
                });

        for (Integer interestIdx : person.getInterests()) {
            String tagSchemaEdgeInterestID = Statement.generateId("S_Tag" + interestIdx);
            String tagEdgeInterestID = Statement.generateId("Tag" + interestIdx);
            writers.get(PERSON_HASINTEREST_TAG)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(personSchemaEdgeID, Relationship.HAS_INTEREST.toString(), tagSchemaEdgeInterestID);
                        edge.as(personEdgeID, Relationship.HAS_INTEREST.toString(), tagEdgeInterestID);
                    });
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt, Person person) {
        String personSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String personEdgeID = Statement.generateId("Person" + person.getAccountId());
        String universitySchemaEdgeID = Statement.generateId("S_University" + studyAt.university);
        String universityEdgeID = Statement.generateId("University" + studyAt.university);

        writers.get(PERSON_STUDYAT_UNIVERSITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(personSchemaEdgeID, Relationship.STUDY_AT.toString(), universitySchemaEdgeID)
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("classYear", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.INTEGER)
                                );
                            });


                    edge.as(personEdgeID, Relationship.STUDY_AT.toString(), universityEdgeID)
                            .withProperties(properties -> {
                                properties.add("classYear", property ->
                                        property.generatePrimitive(PrimitiveType.INTEGER,
                                                DateUtils.formatYear(studyAt.year))
                                );
                            });
                });
    }

    @Override
    protected void serialize(WorkAt workAt, Person person) {
        String personSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String personEdgeID = Statement.generateId("Person" + person.getAccountId());
        String companySchemaEdgeID = Statement.generateId("S_Company" + workAt.company);
        String companyEdgeID = Statement.generateId("Company" + workAt.company);

        writers.get(PERSON_WORKAT_COMPANY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(personSchemaEdgeID, Relationship.WORK_AT.toString(), companySchemaEdgeID)
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("workFrom", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.INTEGER)
                                );
                            });


                    edge.as(personEdgeID, Relationship.WORK_AT.toString(), companyEdgeID)
                            .withProperties(properties -> {
                                properties.add("workFrom", property ->
                                        property.generatePrimitive(PrimitiveType.INTEGER,
                                                Long.toString(workAt.company))
                                );
                            });
                });
    }

    @Override
    protected void serialize(final Person person, Knows knows) {
        String personSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String personEdgeID = Statement.generateId("Person" + person.getAccountId());
        String knowsPersonSchemaEdgeID = Statement.generateId("S_Person" + knows.to()
                .getAccountId());
        String knowsPersonEdgeID = Statement.generateId("Person" + knows.to()
                .getAccountId());

        writers.get(PERSON_KNOWS_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(personSchemaEdgeID, Relationship.KNOWS.toString(), knowsPersonSchemaEdgeID)
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("creationDate", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.DATE_TIME)
                                );
                            });


                    edge.as(personEdgeID, Relationship.KNOWS.toString(), knowsPersonEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME,
                                                DateUtils.formatYear(knows.getCreationDate()))
                                );
                            });
                });
    }
}
