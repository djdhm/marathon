package mesosphere.marathon
package core.health

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.state.Timestamp

case class Health(
    instanceId: Instance.Id,
    firstSuccess: Option[Timestamp] = None,
    lastSuccess: Option[Timestamp] = None,
    lastFailure: Option[Timestamp] = None,
    lastFailureCause: Option[String] = None,
    consecutiveFailures: Int = 0) {

  def alive: Boolean = lastSuccess.exists { successTime =>
    lastFailure.fold(true) { successTime > _ }
  }

  /** Returns true if instance became healthy at least once */
  def ready: Boolean = firstSuccess.isDefined

  def update(result: HealthResult): Health = result match {
    case Healthy(_, _, _, time, _) => copy(
      firstSuccess = firstSuccess.orElse(Some(time)),
      lastSuccess = Some(time),
      consecutiveFailures = 0
    )
    case Unhealthy(_, _, _, cause, time, _) => copy(
      lastFailure = Some(time),
      lastFailureCause = Some(cause),
      consecutiveFailures = consecutiveFailures + 1
    )
  }
}
