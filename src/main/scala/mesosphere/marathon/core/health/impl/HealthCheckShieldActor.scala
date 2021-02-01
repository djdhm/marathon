package mesosphere.marathon
package core.health.impl

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.health.impl.HealthCheckShieldActor._
import mesosphere.marathon.core.health.HealthCheckShieldConf

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{Actor, Props, Timers}

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
      val cacheInitFuture = api.reloadFromRepository()
      // There's no way in Marathon codebase to wait for asynchronous preload
      // By blocking, we give the future at least some time to populate the cache
      Await.result(cacheInitFuture, atMost = 5.seconds)
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
    case Purge =>
      // ensure we have only one purge at a time
      if (lastPurge.isCompleted) {
        lastPurge = api.purgeExpiredShields()
      }
  }
}

object HealthCheckShieldActor {
  case class TimerKey()
  case class Purge()

  def props(api: HealthCheckShieldApiInternal, conf: HealthCheckShieldConf): Props =
    Props(new HealthCheckShieldActor(api, conf))
}
