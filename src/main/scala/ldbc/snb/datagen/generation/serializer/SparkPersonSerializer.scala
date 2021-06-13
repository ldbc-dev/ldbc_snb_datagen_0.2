package ldbc.snb.datagen.generation.serializer

import com.google.common.base.Joiner
import com.google.common.collect.ImmutableList
import ldbc.snb.datagen.DatagenContext
import ldbc.snb.datagen.dictionary.Dictionaries
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.entities.dynamic.relations.{Knows, StudyAt, WorkAt}
import ldbc.snb.datagen.serializer.FileName.{PERSON, PERSON_HASINTEREST_TAG, PERSON_KNOWS_PERSON, PERSON_STUDYAT_UNIVERSITY, PERSON_WORKAT_COMPANY}
import ldbc.snb.datagen.serializer.{DynamicPersonSerializer, FileName, PersonExporter}
import ldbc.snb.datagen.util.{DateUtils, GeneratorConfiguration, Logging, SerializableConfiguration}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.formatter.DateFormatter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobID, RecordWriter, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.execution.datasources.parquet.ExposedParquetWriteSupport

import java.text.SimpleDateFormat
import java.util
import java.util.stream.Collectors
import java.util.{Date, Locale, function}
import scala.collection.JavaConversions._


case class PersonEntry(
              creationDate: java.sql.Timestamp,
              deletionDate: java.sql.Timestamp,
              explicitlyDeleted: Boolean,
              id: Long,
              firstName: String,
              lastName: String,
              `gender`: String,
              `birthday`: java.sql.Date,
              `locationIP`: String,
              `browserUsed`: String,
              `place`: Integer,
              `language`: String,
              `email`: String
            )

case class PersonHasInterestTagEntry(
                            creationDate: Long,
                            deletionDate: Long,
                            personId: Long,
                            interestIdx: Int
                       )

case class PersonKnowsPersonEntry(
                              creationDate: Long,
                              deletionDate: Long,
                              explicitlyDeleted: Boolean,
                              `Person1.Id`: Long,
                              `Person2.Id`: Long
                            )

case class PersonStudyAtUniversityEntry(
                                    creationDate: Long,
                                    deletionDate: Long,
                                    explicitlyDeleted: Boolean,
                                    `Person.id`: Long,
                                    `Organisation.id`: Long,
classYear: Int
                                  )

case class PersonWorkAtCompany(

                           creationDate: Long,
                           deletionDate: Long,
                           explicitlyDeleted: Boolean,
                           `Person.id`: Long,
                           `Organisation.id`: Long,
                           workFrom: Int
                         )

object ParquetWriteSupport {
  implicit def apply[T: WriteSupport] = implicitly[WriteSupport[T]]


  implicit def parquetWriteSupportForEncodables[T <: Product : Encoder] = new WriteSupport[T] {
    val encoder = encoderFor[T]
    val schema = encoder.schema

    val inner = new ExposedParquetWriteSupport(schema)

    override def init(configuration: Configuration): WriteSupport.WriteContext = inner.init(configuration)

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = inner.prepareForWrite(recordConsumer)

    override def write(record: T): Unit = {
      inner.write(encoder.toRow(record))
    }
  }
}

class SplittingRecordWriter[T](
                             files: Seq[Path],
                             taskAttemptContext: TaskAttemptContext,
                             ws: WriteSupport[T]
                           ) extends RecordWriter[Void, T] with Logging {
  var currentFile = 0

  val numFiles = files.length


  val writers = for {f <- files} yield {
    new RecordWriter[Void, T] {
      lazy val inner = {
        val pof = new ParquetOutputFormat[T](ws)
        pof.getRecordWriter(taskAttemptContext, f)
      }
      var hasWritten = false
      override def write(key: Void, value: T): Unit = { hasWritten = true; inner.write(key, value) }
      override def close(context: TaskAttemptContext): Unit = if(hasWritten) { inner.close(context) }
    }

  }

  override def write(key: Void, value: T): Unit = {
    writers(currentFile).write(key, value)
    currentFile = (currentFile + 1) % numFiles
  }

  override def close(context: TaskAttemptContext): Unit = {
    for { w <- writers } {w.close(context)}
  }
}

object SparkPersonSerializer {
    val dateFormatter = new DateFormatter

  def getFile(root: String, entityName: String, partitionId: Long, fileId: Long) = {
    new Path(s"$root/parquet/raw/composite-merged-fk/dynamic/$entityName/part_${partitionId}_${fileId}.parquet")
  }

  def apply(
    persons: RDD[Person],
    conf: GeneratorConfiguration,
    partitions: Option[Int] = None,
    oversizeFactor: Double = 1.0
  )(implicit spark: SparkSession): Unit = {
    val serializableHadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val jobIdInstant = new Date().getTime

    persons
      .pipeFoldLeft(partitions)((rdd: RDD[Person], p: Int) => rdd.coalesce(p))
      .foreachPartition(persons => {

        DatagenContext.initialize(conf)
        val hadoopConf = serializableHadoopConf.value
        val partitionId = TaskContext.getPartitionId()
        val buildDir = conf.getOutputDir

        val fs = FileSystem.get(hadoopConf)
        fs.mkdirs(new Path(buildDir))

        val tac = getTaskAttemptContext(hadoopConf, TaskContext.get(), jobIdInstant)

        val numFiles = 1 //Math.max(1, oversizeFactor.floor.toInt)

        import ParquetWriteSupport._

        import spark.implicits._

        def makeWriter[T<: Product: Encoder](fileName: FileName) = {
          val pws = ParquetWriteSupport[T]

          val files = for { i <- 0 until numFiles } yield {
            getFile(conf.getOutputDir, fileName.name, partitionId, i)
          }

          new SplittingRecordWriter[T](files, tac, pws)
        }

        val personWriter = makeWriter[PersonEntry](PERSON)
        val personKnowsPersonWriter = makeWriter[PersonKnowsPersonEntry](PERSON_KNOWS_PERSON)

        def serializePerson(person: Person): Unit = {
          val pe = PersonEntry(
            new java.sql.Timestamp(person.getCreationDate),
            new java.sql.Timestamp(person.getDeletionDate),
            person.isExplicitlyDeleted,
            person.getAccountId,
            person.getFirstName,
            person.getLastName,
            getGender(person.getGender),
            new java.sql.Date(person.getBirthday),
            person.getIpAddress.toString,
            Dictionaries.browsers.getName(person.getBrowserId),
            person.getCityId,
            buildLanguages(person.getLanguages),
            buildEmail(person.getEmails)
          )
          // creationDate, deletionDate, explicitlyDeleted, id, firstName, lastName, gender, birthday, locationIP, browserUsed, isLocatedIn, language, email
          personWriter.write(null, pe)
          val dates2: util.List[String] = ImmutableList.of(formatDateTime(person.getCreationDate), formatDateTime(person.getDeletionDate))
          import scala.collection.JavaConversions._
          for (interestIdx <- person.getInterests) { // creationDate, deletionDate Person.id, Tag.id
            //writers.get(PERSON_HASINTEREST_TAG).writeEntry(dates2, ImmutableList.of(person.getAccountId.toString, Integer.toString(interestIdx)))
          }
        }

        def serializePersonStudyAt(studyAt: StudyAt, person: Person): Unit = {
          val dates: util.List[String] = ImmutableList.of(formatDateTime(person.getCreationDate), formatDateTime(person.getDeletionDate))
          // creationDate, deletionDate Person.id, University.id, classYear
          //writers.get(PERSON_STUDYAT_UNIVERSITY).writeEntry(dates, ImmutableList.of(studyAt.person.toString, studyAt.university.toString, DateUtils.formatYear(studyAt.year)))
        }

        def serializePersonWorkAt(workAt: WorkAt, person: Person): Unit = {
          val dates: util.List[String] = ImmutableList.of(formatDateTime(person.getCreationDate), formatDateTime(person.getDeletionDate))
          // creationDate, deletionDate Person.id, Company.id, workFrom
          //writers.get(PERSON_WORKAT_COMPANY).writeEntry(dates, ImmutableList.of(workAt.person.toString, workAt.company.toString, DateUtils.formatYear(workAt.year)))
        }

        def serializePersonKnowsPerson(person: Person, knows: Knows): Unit = {
          // creationDate, deletionDate, explicitlyDeleted, Person1.id, Person2.id
          val pkpe = PersonKnowsPersonEntry(person.getCreationDate, person.getDeletionDate, person.isExplicitlyDeleted, person.getAccountId, knows.to.getAccountId)
          //personKnowsPersonWriter.write(null, pkpe)
        }

        def export(person: Person): Unit = {
          serializePerson(person)
          val universityId: Long = Dictionaries.universities.getUniversityFromLocation(person.getUniversityLocationId)
          if ((universityId != -1) && (person.getClassYear != -1)) {
            val studyAt: StudyAt = new StudyAt
            studyAt.year = person.getClassYear
            studyAt.person = person.getAccountId
            studyAt.university = universityId
            serializePersonStudyAt(studyAt, person)
          }
          for (companyId <- person.getCompanies.keySet) {
            val workAt: WorkAt = new WorkAt
            workAt.company = companyId
            workAt.person = person.getAccountId
            workAt.year = person.getCompanies.get(companyId)
            serializePersonWorkAt(workAt, person)
          }
          val knows: util.List[Knows] = person.getKnows
          for (know <- knows) {
            if (person.getAccountId < know.to.getAccountId) serializePersonKnowsPerson(person, know)
          }
        }

        for {p <- persons} {
          export(p)
        }

        personWriter.close(tac)
        personKnowsPersonWriter.close(tac)

      })
  }


  protected def formatDateTime(epochMillis: Long): String = dateFormatter.formatDateTime(epochMillis)
  protected  def formatDate(epochMillis: Long): String = dateFormatter.formatDate(epochMillis)

  private def getGender(gender: Int): String = if (gender == 0) "male" else "female"

  import scala.collection.JavaConverters._

  private def buildLanguages(languages: util.List[Integer]): String = {
    languages.asScala.mkString(";")
  }

  private def buildEmail(emails: util.List[String]): String = Joiner.on(";").join(emails)

  private[this] def getTaskAttemptContext(conf: Configuration,
                                          tc: TaskContext,
                                          jobIdInstant: Long): TaskAttemptContext = {

    val jobId = createJobID(new Date(jobIdInstant), tc.stageId())
    val taskId = new TaskID(jobId, TaskType.MAP, tc.partitionId())
    val taskAttemptId = new TaskAttemptID(taskId, tc.taskAttemptId().toInt & Integer.MAX_VALUE)

    // Set up the attempt context required to use in the output committer.
    {
      // Set up the configuration object
      conf.set("mapreduce.job.id", jobId.toString)
      conf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      conf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      conf.setBoolean("mapreduce.task.ismap", true)
      conf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(conf, taskAttemptId)
    }
  }

  private[this] def createJobID(time: Date, id: Int): JobID = {
    val jobtrackerID = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(time)
    new JobID(jobtrackerID, id)
  }
}

