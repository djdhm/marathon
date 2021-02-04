package mesosphere.marathon
package core.health.impl

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.health.impl.HealthCheckShieldActor._
import mesosphere.marathon.core.health.HealthCheckShieldConf

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{Actor, Props, Timers}

import scala.util.control.NonFatal
import scala.concurrent.TimeoutException
import scala.util.{Success, Failure}

import scala.concurrent.Future
import akka.{Done}

// This actor serves two purposes:
// 1. Detect the leadership change, so that the cache in HealthCheckShieldApi is updated when we become a leader
// 2. Schedule a periodical purge of the expired health check shields
private[health] class HealthCheckShieldActor(
    api: HealthCheckShieldApiInternal,
    conf: HealthCheckShieldConf
) extends Actor with Timers with StrictLogging {
  var lastPurge: Future[Done] = Future.successful(Done)

  override def preStart() = {
    if (conf.healthCheckShieldFeatureEnabled) {
      val loadingTimeout = 5.seconds
      try {
        val cacheInitFuture = api.reloadFromRepository()
        // There's no way in Marathon codebase to wait for asynchronous preload
        // By blocking, we give the future at least some time to populate the cache
        Await.result(cacheInitFuture, atMost = loadingTimeout)
      } catch {
        case (e: TimeoutException) => {
          logger.warn(s"[health-check-shield] Loading of the shields took more than $loadingTimeout")
        }
        case NonFatal(e) => {
          logger.warn("[health-check-shield] Failed to fill cache from storage", e)
        }
      }

      // Trigger the first purge immediately, then purge periodically
      self ! Purge
      schedulePurge()
    } else {
      logger.info("[health-check-shield] Health check shield feature is disabled")
    }
  }

  def schedulePurge() = {
    val purgeInterval = conf.healthCheckShieldPurgePeriod
    timers.startPeriodicTimer(TimerKey, Purge, purgeInterval)
    logger.info(s"[health-check-shield] Scheduled periodical purge with period ${purgeInterval}")
  }

  def receive: Receive = {
    case Purge => {
      // ensure we have only one purge at a time
      lastPurge.value match {
        case Some(Success(_)) => {
          lastPurge = api.purgeExpiredShields()
        }
        case Some(Failure(e)) => {
          logger.warn("[health-check-shield] Failed to purge the shields, trying again...", e)
          lastPurge = api.purgeExpiredShields()
        }
        case None => {
          logger.warn("[health-check-shield] Purging the shields takes a long time, could indicate a problem")
        }
      }
    }
  }
}

object HealthCheckShieldActor {
  case class TimerKey()
  case class Purge()

  def props(api: HealthCheckShieldApiInternal, conf: HealthCheckShieldConf): Props =
    Props(new HealthCheckShieldActor(api, conf))
}
