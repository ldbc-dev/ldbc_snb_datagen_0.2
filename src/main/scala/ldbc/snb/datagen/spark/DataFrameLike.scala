package ldbc.snb.datagen.spark

import ldbc.snb.datagen.transformation.model.{Graph, Mode}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import shapeless._

import scala.languageFeature.implicitConversions

trait DataFrameLike[T] {
  def persist(t: T, storageLevel: StorageLevel): T
}

object DataFrameLike {
  type TC[T] = DataFrameLike[T]
  
  def apply[A](implicit instance: TC[A]): TC[A] = instance

  trait Ops[A] {
    def typeClassInstance: TC[A]
    def self: A
    def persist(storageLevel: StorageLevel): A = typeClassInstance.persist(self, storageLevel)
  }

  object ops {
    implicit def toAllTCOps[A](target: A)(implicit tc: TC[A]): Ops[A] = new Ops[A] {
      val self = target
      val typeClassInstance = tc
    }
  }

  object instances {
    import DataFrameLike.ops._

    implicit def dataFrameLikeInstanceForSimpleGraph[M <: Mode](implicit ev: DataFrame =:= M#Layout[DataFrame]) = new DataFrameLike[Graph[M, DataFrame]] {
      override def persist(t: Graph[M, DataFrame], storageLevel: StorageLevel): Graph[M, DataFrame] = {
        lens[Graph[M, DataFrame]].entities.modify(t)(e => e.map { case (k, v) => (k, ev.apply(v.asInstanceOf[DataFrame].persist(storageLevel))) })
      }
    }

    implicit def dataFrameLikeInstanceForProduct[T <: Product, Repr <: HList : DataFrameLike](implicit repr: Generic.Aux[T, Repr]) = new DataFrameLike[T] {
      override def persist(t: T, storageLevel: StorageLevel): T = repr.from(repr.to(t).persist(storageLevel))
    }

    implicit def dataFrameLikeInstanceForHCons[H : DataFrameLike, T <: HList : DataFrameLike] = new DataFrameLike[H :: T] {
      override def persist(t: H :: T, storageLevel: StorageLevel): H :: T = t.head.persist(storageLevel) :: t.tail.persist(storageLevel)
    }

    implicit val dataFrameLikeInstanceForHNil = new DataFrameLike[HNil] {
      override def persist(t: HNil, storageLevel: StorageLevel): HNil = HNil
    }

    implicit val dataFrameLikeInstanceForDataFrame = new DataFrameLike[DataFrame] {
      override def persist(t: DataFrame, storageLevel: StorageLevel): DataFrame = t.persist(storageLevel)
    }
  }
}


