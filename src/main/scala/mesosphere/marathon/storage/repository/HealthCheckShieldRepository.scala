package mesosphere.marathon
package storage.repository

import mesosphere.marathon.core.health.HealthCheckShield
import mesosphere.marathon.core.task.Task
import akka.util.ByteString
import java.nio.charset.StandardCharsets
import java.nio.ByteOrder
import java.time.Instant
import java.time.OffsetDateTime

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import mesosphere.marathon.core.storage.repository._
import mesosphere.marathon.core.storage.repository.impl.PersistenceStoreRepository
import mesosphere.marathon.core.storage.store.impl.memory.{Identity, RamId}
import mesosphere.marathon.core.storage.store.impl.zk.{ZkId, ZkSerialized}
import mesosphere.marathon.core.storage.store.{IdResolver, PersistenceStore}
import mesosphere.marathon.state.Timestamp

trait HealthCheckShieldRepository
  extends Repository[HealthCheckShield.Id, HealthCheckShield]

object HealthCheckShieldRepository {
  implicit val category = "HealthCheckShield"

  val v1ShieldName = "v1_shield"
  val v2StoragePrefix = "v2_"

  implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  implicit val zkIdResolver = new IdResolver[HealthCheckShield.Id, HealthCheckShield, String, ZkId] {
    def toStorageId(id: HealthCheckShield.Id, version: Option[OffsetDateTime]): ZkId = {
      if (id.shieldName != v1ShieldName) {
        ZkId(category, s"${v2StoragePrefix}${id.idString}", version)
      } else {
        // v1 is supported in key write path to be able to delete v1 shields from Zk after read
        ZkId(category, id.taskId.idString, version)
      }
    }
    def fromStorageId(key: ZkId): HealthCheckShield.Id = {
      if (key.id.startsWith(v2StoragePrefix)) {
        HealthCheckShield.Id.parse(key.id.substring(v2StoragePrefix.length))
      } else {
        val taskId = Task.Id.parse(key.id)
        HealthCheckShield.Id(taskId, v1ShieldName)
      }
    }
    val hasVersions: Boolean = false
    val category: String = HealthCheckShieldRepository.category
    def version(v: HealthCheckShield): OffsetDateTime =
      Timestamp.now().toOffsetDateTime
  }

  implicit val zkMarshaller: Marshaller[HealthCheckShield, ZkSerialized] =
    Marshaller.opaque { (shield: HealthCheckShield) =>
      val bytes = ByteString.newBuilder
      val serializationVersion = 2
      bytes.putInt(serializationVersion)
      val shieldIdBytes = shield.id.idString.getBytes(StandardCharsets.UTF_8)
      bytes.putInt(shieldIdBytes.length)
      bytes.putBytes(shieldIdBytes)
      bytes.putLong(shield.until.toInstant.toEpochMilli)
      ZkSerialized(bytes.result)
    }

  implicit val zkUnmarshaller = Unmarshaller.strict { (zk: ZkSerialized) =>
    val it = zk.bytes.iterator
    val serializationVersion = it.getInt
    if (serializationVersion == 1) {
      val taskIdLength = it.getInt
      val taskIdString = new String(it.getBytes(taskIdLength), StandardCharsets.UTF_8)
      val taskId = Task.Id.parse(taskIdString)
      val until = Instant.ofEpochMilli(it.getLong)
      HealthCheckShield(HealthCheckShield.Id(taskId, v1ShieldName), Timestamp(until))
    } else if (serializationVersion == 2) {
      val sheildIdLength = it.getInt
      val shieldIdString = new String(it.getBytes(sheildIdLength), StandardCharsets.UTF_8)
      val until = Instant.ofEpochMilli(it.getLong)
      HealthCheckShield(HealthCheckShield.Id.parse(shieldIdString), Timestamp(until))
    } else {
      throw new MatchError(s"unsupported heatlh check shield serialization version ${serializationVersion}")
    }
  }

  def zkRepository(persistenceStore: PersistenceStore[ZkId, String, ZkSerialized]): HealthCheckShieldRepository = {
    new HealthCheckShieldRepositoryImpl(persistenceStore)
  }

  implicit val inMemIdResolver = new IdResolver[HealthCheckShield.Id, HealthCheckShield, String, RamId] {
    def toStorageId(id: HealthCheckShield.Id, version: Option[OffsetDateTime]): RamId =
      RamId(category, id.idString, version)
    def fromStorageId(key: RamId): HealthCheckShield.Id = HealthCheckShield.Id.parse(key.id)
    val hasVersions: Boolean = false
    val category: String = HealthCheckShieldRepository.category
    def version(v: HealthCheckShield): OffsetDateTime =
      Timestamp.now().toOffsetDateTime
  }

  def inMemRepository(persistenceStore: PersistenceStore[RamId, String, Identity]): HealthCheckShieldRepository = {
    import mesosphere.marathon.storage.store.InMemoryStoreSerialization._
    new HealthCheckShieldRepositoryImpl(persistenceStore)
  }
}

class HealthCheckShieldRepositoryImpl[K, C, S](persistenceStore: PersistenceStore[K, C, S])(
    implicit
    ir: IdResolver[HealthCheckShield.Id, HealthCheckShield, C, K],
    marshaller: Marshaller[HealthCheckShield, S],
    unmarshaller: Unmarshaller[S, HealthCheckShield]
) extends PersistenceStoreRepository[HealthCheckShield.Id, HealthCheckShield, K, C, S](
  persistenceStore,
  _.id) with HealthCheckShieldRepository