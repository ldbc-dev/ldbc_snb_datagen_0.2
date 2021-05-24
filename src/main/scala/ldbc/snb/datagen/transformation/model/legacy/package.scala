package ldbc.snb.datagen.transformation.model

package object legacy {
  case class Place(
    id: Int,
    zId: Int,
    name: String,
    latitude: Double,
    longitude: Double,
    population: Long,
    `type`: String
  )

  case class Organisation(
    id: Long,
    name: String,
    `type`: Organisation.Type.Value,
    location: Int
  )

  object Organisation {
    abstract class Type extends Enumeration
    object Type extends Type {
      val University, Company = Value
    }
  }

  case class Tag(
    id: Int,
    tagClass: Int,
    name: String
  )

  case class TagClass(
    id: Int,
    name: String,
    parent: Int
  )

  case class Person(
    isExplicitlyDeleted: Boolean,
    isMessageDeleter: Boolean,
    accountId: Long,
    creationDate: Long,
    deletionDate: Long,
    maxNumKnows: Long,
    knows: Set[Knows],
    browserId: Int,
    ipAddress: IP,
    countryId: Int,
    cityId: Int,
    interests: Set[Int],
    mainInterest: Int,
    universityLocationId: Int,
    gender: Byte,
    birthday: Long,
    isLargePoster: Boolean,
    randomId: Long,
    emails: Set[String],
    languages: List[Int],
    firstName: String,
    lastName: String,
    companies: Map[Long, Long],
    classYear: Long
  )

  case class IP(ip: Int, mask: Int) {

    import IP._

    def format: String =
      ((ip >>> BYTE1_SHIFT_POSITION) & BYTE_MASK) + "." +
        ((ip >>> BYTE2_SHIFT_POSITION) & BYTE_MASK) + "." +
        ((ip >>> BYTE3_SHIFT_POSITION) & BYTE_MASK) + "." +
        (ip & BYTE_MASK)
  }

  object IP {
    val BYTE_MASK = 0xFF
    val IP4_SIZE_BITS = 32
    val BYTE1_SHIFT_POSITION = 24
    val BYTE2_SHIFT_POSITION = 16
    val BYTE3_SHIFT_POSITION = 8
    val BYTE4_SHIFT_POSITION = 0
  }

  case class Knows(
    isExplicitlyDeleted: Boolean,
    to: PersonSummary,
    creationDate: Long,
    deletionDate: Long,
    weight: Float
  )

  case class PersonSummary(
    accountId: Long,
    creationDate: Long,
    deletionDate: Long,
    browserId: Int,
    country: Int,
    ipAddress: IP,
    isLargePoster: Boolean,
    isMessageDeleter: Boolean
  )

  case class ForumMembership(
    isExplicitlyDeleted: Boolean,
    forumId: Long,
    creationDate: Long,
    deletionDate: Long,
    person: PersonSummary,
    forumType: Forum.Type.Value
  )

  case class Forum(
    isExplicitlyDeleted: Boolean,
    id: Long,
    moderator: PersonSummary,
    moderatorDeletionDate: Long,
    creationDate: Long,
    deletionDate: Long,
    title: String,
    tags: List[Int],
    placeId: Int,
    language: Int,
    memberships: List[ForumMembership],
    forumType: Forum.Type.Value
  )

  object Forum {

    abstract class Type extends Enumeration

    object Type extends Type {
      val Wall, Album, Group = Value
    }

  }

  case class Like(
    isExplicitlyDeleted: Boolean,
    person: Long,
    personCreationDate: Long,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    `type`: Like.Type.Value
  )

  object Like {

    abstract class Type extends Enumeration

    object Type extends Type {
      val Post, Comment, Photo = Value
    }

  }

  case class Comment(
    isExplicitlyDeleted: Boolean,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    author: PersonSummary,
    forumId: Long,
    content: String,
    tags: Set[Int],
    ipAddress: IP,
    browserId: Int,
    countryId: Int,
    rootPostId: Long,
    parentMessageId: Long
  )

  case class Post(
    isExplicitlyDeleted: Boolean,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    author: PersonSummary,
    forumId: Long,
    content: String,
    tags: Set[Int],
    ipAddress: IP,
    browserId: Int,
    countryId: Int,
    language: Int
  )

  case class Photo(
    isExplicitlyDeleted: Boolean,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    author: PersonSummary,
    forumId: Long,
    content: String,
    tags: Set[Int],
    ipAddress: IP,
    browserId: Int,
    countryId: Int
  )

  type Wall[A] = List[(Forum, List[ForumMembership], List[A])]
  type PostTree = (Post, List[Like], List[(Comment, List[Like])])
  type PhotoTree = (Photo, List[Like])

  case class Activity(
    wall: Wall[PostTree],
    groups: List[Wall[PostTree]],
    albums: Wall[PhotoTree]
  )
}


