package mesosphere.marathon
package core.health

import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.core.task.Task

case class HealthCheckShield(id: HealthCheckShield.Id, until: Timestamp) {
}

object HealthCheckShield {
  case class Id(taskId: Task.Id, shieldName: String) {
    lazy val idString = s"${taskId.idString}.${shieldName}"

    override def toString: String = s"shield [${idString}]"
  }

  object Id {
    private val IdRegex = """^(.+)[\.]([^\.]+)$""".r

    def parse(idString: String): HealthCheckShield.Id = {
      idString match {
        case IdRegex(taskIdString, shieldName) => {
          try {
            val taskId = Task.Id.parse(taskIdString)
            Id(taskId, shieldName)
          } catch {
            case e: MatchError => {
              throw new MatchError(s"Error while parsing shieldId '$idString': '$e'")
            }
          }
        }
        case _ => throw new MatchError(s"shieldId '$idString' unrecognized format")
      }
    }
  }
}