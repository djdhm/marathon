package mesosphere.marathon
package raml

trait HealthConversion extends DefaultConversions {

  implicit val healthRamlWriter: Writes[core.health.Health, Health] = Writes { health =>
    Health(
      alive = health.alive,
      consecutiveFailures = health.consecutiveFailures,
      firstSuccess = health.firstSuccess.toRaml,
      instanceId = health.instanceId.toRaml,
      lastSuccess = health.lastSuccess.toRaml,
      lastFailure = health.lastFailure.toRaml,
      lastFailureCause = health.lastFailureCause
    )
  }

  implicit val healthCheckShieldRamlWriter: Writes[core.health.HealthCheckShield, HealthCheckShield] = Writes { hcs =>
    HealthCheckShield(
      taskId = hcs.id.taskId.idString,
      shieldName = hcs.id.shieldName,
      until = hcs.until.toRaml
    )
  }
}

object HealthConversion extends HealthConversion
