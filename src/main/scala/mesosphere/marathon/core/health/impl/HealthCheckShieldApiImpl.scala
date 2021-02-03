package mesosphere.marathon
package core.health.impl

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.core.health.{HealthCheckShield, HealthCheckShieldConf, HealthCheckShieldApi}
import mesosphere.marathon.storage.repository.HealthCheckShieldRepository
import mesosphere.marathon.util.KeyedLock

import scala.collection.concurrent.TrieMap
import scala.async.Async.{async, await}
import akka.stream.scaladsl.{Source, Sink}
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import akka.{Done}
import scala.concurrent.ExecutionContext

trait HealthCheckShieldApiInternal extends HealthCheckShieldApi {
  // Maintenance operations used during the bootstrup of the shield api
  def purgeExpiredShields(): Future[Done]
  def reloadFromRepository(): Future[Done]
}

/* Provides asynchronous interface to health check shield info

   Synchronizes between the in-memory representation (HeatlhCheckShieldCache)
   and persistent storage in ZooKeeper (HealthCheckShieldRepository)
*/
final class HealthCheckShieldApiImpl(
    repository: HealthCheckShieldRepository,
    conf: HealthCheckShieldConf
)(implicit ec: ExecutionContext, mat: ActorMaterializer)
  extends HealthCheckShieldApiInternal with StrictLogging {

  private val cache = new HealthCheckShieldCache()
  // Accepts a key (taskId), and a "=> Future" as a parameter
  // Limits concurrency per key to 1
  private val lock = KeyedLock[Task.Id]("HealthCheckShieldApiImpl", Int.MaxValue)

  def isShielded(taskId: Task.Id): Boolean = {
    if (conf.healthCheckShieldFeatureEnabled) {
      cache.isShielded(taskId)
    } else {
      false
    }
  }

  def addOrUpdate(shield: HealthCheckShield): Future[Done] = {
    lock(shield.id.taskId) {
      async {
        await (repository.store(shield))
        cache.addOrUpdate(shield)
        logger.info(s"[health-check-shield] Enabled ${shield.id} until ${shield.until}")
        Done
      }
    }
  }

  def remove(shieldId: HealthCheckShield.Id): Future[Done] = {
    lock(shieldId.taskId) {
      async {
        await (repository.delete(shieldId))
        cache.remove(shieldId)
        logger.info(s"[health-check-shield] Disabled ${shieldId}")
        Done
      }
    }
  }

  def getShield(shieldId: HealthCheckShield.Id): Future[Option[HealthCheckShield]] = {
    Future.successful(cache.getShield(shieldId))
  }
  def getShields(taskId: Task.Id): Future[Seq[HealthCheckShield]] = {
    Future.successful(cache.getShields(taskId))
  }
  def getShields(): Future[Seq[HealthCheckShield]] = {
    Future.successful(cache.getShields())
  }

  def purgeExpiredShields(): Future[Done] = async {
    logger.info("[health-check-shield] Purging expired shields...")
    val expired = Source(cache
      .getShields()
      .filter(s => s.until < Timestamp.now()))
    var purgedCount = 0
    val purgeFuture = expired
      .mapAsync(parallelism = 2)(shield => {
        tryPurgeShield(shield.id)
      })
      .runForeach((purged: Boolean) => {
        purgedCount = purgedCount + 1
      })
    await (purgeFuture)
    logger.info(s"[health-check-shield] Purged ${purgedCount} expired shields")
    Done
  }

  private def tryPurgeShield(shieldId: HealthCheckShield.Id): Future[Boolean] = {
    lock(shieldId.taskId) {
      val expired = cache.getShield(shieldId).exists(s => s.until < Timestamp.now())
      if (expired) {
        async {
          logger.info(s"[health-check-shield] Purging ${shieldId}...");
          await (repository.delete(shieldId))
          cache.remove(shieldId)
          true
        }
      } else {
        Future.successful(false)
      }
    }
  }

  def reloadFromRepository(): Future[Done] = {
    logger.info(s"[health-check-shield] Loading shields from Zk...");

    // Safe to call cache.clear without lock as it happens at initialization phase
    cache.clear()

    var count = 0
    val cacheFillSink = Sink.foreach((shield: HealthCheckShield) => {
      cache.addOrUpdate(shield)
      count = count + 1
    })
    val cacheFillFuture = repository.all().runWith(cacheFillSink)

    cacheFillFuture
      .map(result => {
        logger.info(s"[health-check-shield] Loaded ${count} shields");
        Done
      })
      .recover({
        case e: Exception => {
          logger.info(s"[health-check-shield] Loading failed", e)
          Done
        }
      })
  }

  /*
    Provides synchronous interface to the health check shield info stored in memory
    isShielded method is thread-safe, everything else requires some kind of synchronization
  */
  private class HealthCheckShieldCache extends StrictLogging {
    val maxExpirationByTaskId = TrieMap.empty[Task.Id, Timestamp]
    val shieldsByTaskId = TrieMap.empty[Task.Id, TrieMap[HealthCheckShield.Id, HealthCheckShield]]

    def isShielded(taskId: Task.Id): Boolean = {
      maxExpirationByTaskId
        .get(taskId)
        .exists(until => {
          // removes only if 'until' is still the same
          if (until < Timestamp.now() && maxExpirationByTaskId.remove(taskId, until)) {
            logger.info(s"[health-check-shield] All shields expired for ${taskId}")
            false
          } else {
            true
          }
        })
    }

    def clear(): Unit = {
      maxExpirationByTaskId.clear()
      shieldsByTaskId.clear()
    }

    def addOrUpdate(shield: HealthCheckShield): Unit = {
      shieldsByTaskId
        .getOrElseUpdate(shield.id.taskId, TrieMap.empty[HealthCheckShield.Id, HealthCheckShield])
        .update(shield.id, shield)
      updateMaxExpirationIfAny(shield.id.taskId)
    }

    def remove(shieldId: HealthCheckShield.Id): Unit = {
      shieldsByTaskId
        .get(shieldId.taskId)
        .map(shields => {
          shields.remove(shieldId)
          if (shields.isEmpty) {
            shieldsByTaskId.remove(shieldId.taskId)
          }
          updateMaxExpirationIfAny(shieldId.taskId)
        })
    }

    def getShields(): Seq[HealthCheckShield] = {
      shieldsByTaskId
        .values
        .flatMap(shields => shields.values)
        .to[Seq]
    }

    def getShields(taskId: Task.Id): Seq[HealthCheckShield] = {
      shieldsByTaskId.get(taskId) match {
        case Some(shields) => {
          shields.values.to[Seq]
        }
        case None => {
          Seq.empty[HealthCheckShield]
        }
      }
    }

    def getShield(shieldId: HealthCheckShield.Id): Option[HealthCheckShield] = {
      shieldsByTaskId
        .get(shieldId.taskId)
        .flatMap(shields => {
          shields.get(shieldId)
        })
    }

    private def updateMaxExpirationIfAny(taskId: Task.Id): Unit = {
      shieldsByTaskId.get(taskId) match {
        case Some(shields) => {
          val maxUntil = shields.map(s => s._2.until).max
          maxExpirationByTaskId.update(taskId, maxUntil)
        }
        case None => {
          maxExpirationByTaskId.remove(taskId)
        }
      }
    }
  }
}
