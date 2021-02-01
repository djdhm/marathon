package mesosphere.marathon
package api.v2

import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.container.{AsyncResponse, Suspended}
import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.Response
import javax.ws.rs.core.{Context, MediaType}
import mesosphere.marathon.api._
import mesosphere.marathon.core.health.{HealthCheckShieldApi, HealthCheckShield}
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.plugin.auth.{Authenticator, Authorizer}
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.raml.HealthConversion._
import mesosphere.marathon.api.RestResource.RestStreamingBody
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.state.Timestamp

import scala.async.Async._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

@Path("v2/shield")
@Produces(Array(MediaType.APPLICATION_JSON))
class HealthCheckShieldResource @Inject() (
    val config: MarathonConf,
    val healthCheckShieldApi: HealthCheckShieldApi,
    val groupManager: GroupManager,
    val authenticator: Authenticator,
    val authorizer: Authorizer)(implicit val executionContext: ExecutionContext) extends AuthResource with StrictLogging {

  @GET
  def getShields(
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        implicit val identity = await(authenticatedAsync(req))
        val shields = await (healthCheckShieldApi.getShields())
        ok(Raml.toRaml(shields))
      } else {
        featureDisabledError()
      }
    }
  }

  @GET
  @Path("{taskId}")
  def getShields(
    @PathParam("taskId") taskId: String,
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        implicit val identity = await(authenticatedAsync(req))
        val parsedTaskId = Task.Id.parse(taskId)
        val shields = await (healthCheckShieldApi.getShields(parsedTaskId))
        ok(Raml.toRaml(shields))
      } else {
        featureDisabledError()
      }
    }
  }

  @GET
  @Path("{taskId}/{shieldName}")
  def getShield(
    @PathParam("taskId") taskId: String,
    @PathParam("shieldName") shieldName: String,
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        implicit val identity = await(authenticatedAsync(req))
        val parsedTaskId = Task.Id.parse(taskId)
        val shieldId = HealthCheckShield.Id(parsedTaskId, shieldName)
        val maybeShield = await (healthCheckShieldApi.getShield(shieldId))
        maybeShield match {
          case Some(shield) => {
            ok(Raml.toRaml(shield))
          }
          case None => {
            Response.status(Status.NOT_FOUND).build()
          }
        }
      } else {
        featureDisabledError()
      }
    }
  }

  @PUT
  @Path("{taskId}/{shieldName}")
  def putHealthShield(
    @PathParam("taskId") taskId: String,
    @PathParam("shieldName") shieldName: String,
    @QueryParam("duration")@DefaultValue("30 minutes") duration: String,
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        val parsedTaskId = Task.Id.parse(taskId)
        val maybeRunSpec = groupManager.runSpec(parsedTaskId.runSpecId)

        if (maybeRunSpec.isDefined) {
          implicit val identity = await(authenticatedAsync(req))
          if (config.healthCheckShieldAuthorizationEnabled) {
            checkAuthorization(UpdateRunSpec, maybeRunSpec.get)
          }

          val parsedDuration = Duration(duration)
          if (parsedDuration.isFinite() && parsedDuration < config.healthCheckShieldMaxDuration) {
            val ttl = parsedDuration.asInstanceOf[FiniteDuration]
            val shield = HealthCheckShield(HealthCheckShield.Id(parsedTaskId, shieldName), Timestamp.now() + ttl)
            await (healthCheckShieldApi.addOrUpdate(shield))
            ok()
          } else {
            Response.status(Status.BAD_REQUEST).entity(new RestStreamingBody(raml.Error(s"The duration should be finite and less than ${config.healthCheckShieldMaxDuration.toMinutes} minutes"))).build()
          }
        } else {
          unknownApp(parsedTaskId.runSpecId)
        }
      } else {
        featureDisabledError()
      }
    }
  }

  @DELETE
  @Path("{taskId}/{shieldName}")
  def removeHealthShield(
    @PathParam("taskId") taskId: String,
    @PathParam("shieldName") shieldName: String,
    @Context req: HttpServletRequest, @Suspended asyncResponse: AsyncResponse): Unit = sendResponse(asyncResponse) {
    async {
      if (config.healthCheckShieldFeatureEnabled) {
        val parsedTaskId = Task.Id.parse(taskId)

        // We should be able to delete the shield with the API even after the app and its runspec are deleted
        // As we can't authorize something that doesn't exist, delete the shield of unexistent app is allowed after authentication
        val maybeRunSpec = groupManager.runSpec(parsedTaskId.runSpecId)
        implicit val identity = await(authenticatedAsync(req))
        if (maybeRunSpec.isDefined && config.healthCheckShieldAuthorizationEnabled) {
          checkAuthorization(UpdateRunSpec, maybeRunSpec.get)
        }
        val shieldId = HealthCheckShield.Id(parsedTaskId, shieldName)
        await (healthCheckShieldApi.remove(shieldId))
        ok()
      } else {
        featureDisabledError()
      }
    }
  }

  def featureDisabledError(): Response = {
    Response.status(Status.FORBIDDEN).entity(new RestStreamingBody(raml.Error("HealthCheckShield feature is disabled in Marathon config"))).build()
  }
}
