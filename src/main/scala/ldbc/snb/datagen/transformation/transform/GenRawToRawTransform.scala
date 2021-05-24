package ldbc.snb.datagen.transformation.transform


import ldbc.snb.datagen.serializer.FileName
import org.apache.spark.sql.functions._
import ldbc.snb.datagen.transformation.model.{Graph, Mode, legacy}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.transformation.model.EntityType.{Attr, Edge, Node}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types._

case class GenRawToRawTransform(numThreads: Option[Int])(implicit spark: SparkSession) extends Transform[Mode.GenRaw.type, Mode.Raw.type] {
  override def transform(input: In): Out = {
    // as a rule of thumb try to cache every dataset that is used more than once
    // and is close to the leaf

    val parallelism = numThreads.getOrElse(spark.sparkContext.defaultParallelism)

    val legacyPersons = input.entities(Node("Person")).cache()
    val legacyActivities = input.entities(Node("Activity"))

    val temporalAttributes = Seq(
      $"creationDate".cast(TimestampType), // map to date
      $"deletionDate".cast(TimestampType), // map to date
      $"explicitlyDeleted"
    )

    val formatIP = (ip: Column) => {
      val getByte = (address: Column, pos: Int) =>
        pmod(shiftLeft(address, pos).cast(ByteType), lit(256))

      val address = ip.getField("ip")
      format_string(
        "%d.%d.%d.%d",
        getByte(address, legacy.IP.BYTE1_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE2_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE3_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE4_SHIFT_POSITION)
      )
    }

    val cached = (df: DataFrame) => df.cache()

    val personalWall = legacyActivities
      .select(explode($"wall.inner").as("wall"))
      .select($"wall.*")

    val groupWall = legacyActivities
      .select(explode($"groups").as("group"))
      .select(explode($"group.inner").as("wall"))
      .select($"wall.*")

    val textWalls = (personalWall |+| groupWall).cache()

    val photoWall = legacyActivities
      .select(explode($"albums.inner").as("album"))
      .select($"album.*")
      .cache()

    val forumFromWall = (wall: DataFrame) => wall
      .select($"value0.*")
      .cache()

    val forumMembershipFromWall = (wall: DataFrame) => wall
      .select(explode($"value1").as("fm"))
      .select($"fm.*")

    val treeFromWall = (wall: DataFrame) => wall
      .select(explode($"value2").as("pt"))
      .select($"pt.*")

    val postFromTree = (pt: DataFrame) => pt.select($"value0.*")

    val photoFromTree = (pt: DataFrame) => pt.select($"value0.*")

    val likesFromTree = (pt: DataFrame) => pt
      .select(explode($"value1").as("value1"))
      .select($"value1.*")

    val commentTreeFromTree = (pt: DataFrame) => pt
      .select(explode($"value2").as("value2"))
      .select($"value2.*")

    val commentFromCommentTree = (ct: DataFrame) => ct
      .select($"value0.*")

    val commentFromWall = cached compose commentFromCommentTree compose commentTreeFromTree compose treeFromWall

    val comment = commentFromWall(textWalls)
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("id"),
          formatIP($"ipAddress").as("locationIP"),
          $"browserId".as("browserUsed"), // join small dict
          $"content".as("content"),
          length($"content").as("length"),
          $"author.accountId".as("creator"),
          $"countryId".as("place"),
          when($"rootPostId" === $"parentMessageId", $"parentMessageId")
            .as("replyOfPost"),
          when($"rootPostId" =!= $"parentMessageId", $"parentMessageId")
            .as("replyOfComment")
        )
      )

    val commentHasTagTag = commentFromWall(textWalls)
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Comment.id"),
          explode($"tags").as("Tag.id")
        )
      )

    val forum = (forumFromWall(textWalls) |+| forumFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"id",
          $"title",
          $"forumType".as("type"), // map to string
          $"moderator.accountId".as("moderator")
        )
      )

    val forumContainerOfPost = postFromTree(treeFromWall(textWalls))
      .select(
        temporalAttributes ++ Seq(
          $"forumId".as("Forum.id"),
          $"messageId".as("Post.Id")
        )
      )

    val forumHasMemberPerson = (forumMembershipFromWall(textWalls) |+| forumMembershipFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"forumId".as("Forum.id"),
          $"person.accountId".as("Person.id"),
          $"forumType".as("type") // map to string
        )
      )

    val forumHasTagTag = (forumFromWall(textWalls) |+| forumFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"id".as("Forum.id"),
          explode($"tags").as("Tag.id")
        )
      )

    val person = legacyPersons
      .select(
        temporalAttributes ++
          Seq(
            $"accountId".as("id"),
            $"firstName",
            $"lastName",
            $"gender", // map to string
            $"birthday", // map to date
            formatIP($"ipAddress").as("locationIP"),
            $"browserId".as("browserUsed"), // join small dictionary
            $"cityId".as("place"),
            array_join($"languages", ";").as("language"),
            array_join($"emails", ";").as("email")
          )
      )

    val personHasInterestTag = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          explode($"interests").as("Tag.id")
        )
      )

    val personKnowsPerson = legacyPersons
      .select(
        Seq(
          $"creationDate",
          $"deletionDate",
          $"explicitlyDeleted",
          $"accountId",
          explode($"knows").as("know")
        )
      )
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person1.id"),
          $"know.to.accountId".as("Person2.id"),
          $"know.weight".as("weight")
        )
      )
      .where($"`Person1.id`" < $"`Person2.id`")

    val personLikesComment = likesFromTree(commentTreeFromTree(treeFromWall(textWalls)))
      .select(temporalAttributes ++ Seq(
        $"person".as("Person.id"),
        $"messageId".as("Comment.id")
      ))

    val likesFromWall = likesFromTree compose treeFromWall

    val personLikesPost = (likesFromWall(textWalls) |+| likesFromWall(photoWall))
      .select(temporalAttributes ++ Seq(
        $"person".as("Person.id"),
        $"messageId".as("Post.id")
      ))

    val personStudyAtUniversity = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          $"universityLocationId".as("University.id"), // join small dictionary
          $"classYear" // format year
        )
      )
      .where($"`University.id`" =!= -1)

    val personWorkAtCompany = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          explode($"companies").as(Seq("Company.id", "workFrom"))
        )
      )

    val textPost = postFromTree(treeFromWall(textWalls))
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("id"),
          lit(null).as("imageFile"),
          formatIP($"ipAddress").as("locationIP"),
          $"browserId".as("browserUsed"), // join small dict
          $"language".as("language"), // join small dict
          $"content".as("content"),
          length($"content").as("length"),
          $"author.accountId".as("creator"),
          $"forumId".as("Forum.id"),
          $"countryId".as("Place.id")
        )
      )

    val postTagFromPost = (post: DataFrame) => post
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Post.id"),
          explode($"tags").as("Tag.id")
        )
      )

    val postFromWall = cached compose postFromTree compose treeFromWall

    val postHasTagTag =
      postTagFromPost(postFromWall(textWalls)) |+|
        postTagFromPost(postFromWall(photoWall))

    val photoPost = photoFromTree(treeFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("id"),
          $"content".as("imageFile"),
          formatIP($"ipAddress").as("locationIP"),
          $"browserId".as("browserUsed"), // join small dict
          lit(null).as("language"), // join small dict
          lit(null).as("content"),
          lit(0).as("length"),
          $"author.accountId".as("creator"),
          $"forumId".as("Forum.id"),
          $"countryId".as("Place.id")
        )
      )

    val post = textPost |+| photoPost

    Graph[Mode.Raw.type, DataFrame](false, false, Mode.Raw, Map(
      Node("Comment") -> comment.repartition(Math.ceil(FileName.COMMENT.size * parallelism).toInt),
      Edge("HasTag", "Comment", "Tag", NN) -> commentHasTagTag,
      Node("Forum") -> forum,
      Edge("ContainerOf", "Forum", "Post", NN) -> forumContainerOfPost, // cardinality?
      Edge("HasMember", "Forum", "Person", NN) -> forumHasMemberPerson,
      Edge("HasTag", "Forum", "Tag", NN) -> forumHasTagTag,
      Node("Person") -> person,
      Edge("HasInterest", "Person", "Tag", NN) -> personHasInterestTag,
      Edge("Knows", "Person", "Person", NN) -> personKnowsPerson,
      Edge("Likes", "Person", "Comment", NN) -> personLikesComment,
      Edge("Likes", "Person", "Post", NN) -> personLikesPost,
      Edge("StudyAt", "Person", "University", OneN) -> personStudyAtUniversity,
      Edge("WorkAt", "Person", "Company", NN) -> personWorkAtCompany,
      Node("Post") -> post,
      Edge("HasTag", "Post", "Tag", NN) -> postHasTagTag
    ))
  }
}
