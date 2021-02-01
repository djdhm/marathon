package mesosphere.marathon
package core.health

import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.Timestamp

import scala.concurrent.duration._

import scala.concurrent.Future
import akka.{Done}

trait HealthCheckShieldApi {
  // Thread-safe and scalable read path used during health check execution
  def isShielded(taskId: Task.Id): Boolean

  // User API surface
  def getShields(): Future[Seq[HealthCheckShield]]
  def getShields(taskId: Task.Id): Future[Seq[HealthCheckShield]]
  def getShield(shieldId: HealthCheckShield.Id): Future[Option[HealthCheckShield]]

  def addOrUpdate(shield: HealthCheckShield): Future[Done]
  def remove(shieldId: HealthCheckShield.Id): Future[Done]
}