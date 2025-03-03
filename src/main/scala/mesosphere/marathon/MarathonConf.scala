package mesosphere.marathon

import com.typesafe.scalalogging.StrictLogging
import java.net.URL

import mesosphere.marathon.core.appinfo.AppInfoConfig
import mesosphere.marathon.core.deployment.DeploymentConfig
import mesosphere.marathon.core.event.EventConf
import mesosphere.marathon.core.flow.LaunchTokenConfig
import mesosphere.marathon.core.group.GroupManagerConfig
import mesosphere.marathon.core.heartbeat.MesosHeartbeatMonitor
import mesosphere.marathon.core.launcher.OfferProcessorConfig
import mesosphere.marathon.core.launchqueue.{LaunchQueueConfig, ReviveOffersConfig}
import mesosphere.marathon.core.matcher.manager.OfferMatcherManagerConfig
import mesosphere.marathon.core.plugin.PluginManagerConfiguration
import mesosphere.marathon.core.task.jobs.TaskJobsConfig
import mesosphere.marathon.core.task.termination.KillConfig
import mesosphere.marathon.core.task.tracker.InstanceTrackerConfig
import mesosphere.marathon.core.task.update.TaskStatusUpdateConfig
import mesosphere.marathon.metrics.MetricsConf
import mesosphere.marathon.state.Role
import mesosphere.marathon.state.ResourceRole
import mesosphere.marathon.storage.StorageConf
import mesosphere.mesos.MatcherConf
import org.rogach.scallop.{ScallopConf, ScallopOption, ValueConverter}
import mesosphere.marathon.core.health.HealthCheckShieldConf

import scala.sys.SystemProperties

/**
  * We have to cache in a separated object because [[mesosphere.marathon.MarathonConf]] is a trait and thus every
  * instance would call {{{java.net.InetAddress.getLocalHost}}} which is blocking. We want to call it only once.
  * Note: This affect mostly tests.
  */
private[marathon] object MarathonConfHostNameCache {

  lazy val hostname = java.net.InetAddress.getLocalHost.getHostName
}

trait MarathonConf
  extends ScallopConf
  with EventConf with NetworkConf with FeaturesConf with GroupManagerConfig with LaunchQueueConfig with LaunchTokenConfig
  with LeaderProxyConf with MarathonSchedulerServiceConfig with OfferMatcherManagerConfig with OfferProcessorConfig
  with PluginManagerConfiguration with ReviveOffersConfig with StorageConf with KillConfig
  with TaskJobsConfig with TaskStatusUpdateConfig with InstanceTrackerConfig with DeploymentConfig with ZookeeperConf
  with MatcherConf with AppInfoConfig with MetricsConf with HealthCheckShieldConf {

  import MarathonConf._
  lazy val mesosMaster = opt[MesosMasterConnection](
    "master",
    descr = "The URL of the Mesos master. Either http://host:port, or zk://host1:port1,host2:port2,.../path",
    required = true,
    noshort = true)(mesosMasterValueConverter)

  lazy val mesosLeaderUiUrl = opt[String](
    "mesos_leader_ui_url",
    descr = "The host and port (e.g. \"http://mesos_host:5050\") of the Mesos master.",
    required = false,
    noshort = true)

  lazy val mesosFailoverTimeout = opt[Long](
    "failover_timeout",
    descr = "(Default: 1 week) The failover_timeout for mesos in seconds.",
    default = Some(604800L))

  lazy val highlyAvailable = toggle(
    "ha",
    descrYes = "(Default) Run Marathon in HA mode with leader election. " +
      "Allows starting an arbitrary number of other Marathons but all need " +
      "to be started in HA mode. This mode requires a running ZooKeeper",
    descrNo = "Run Marathon in single node mode.",
    prefix = "disable_",
    noshort = true,
    default = Some(true))

  lazy val checkpoint = toggle(
    "checkpoint",
    descrYes = "(Default) Enable checkpointing of tasks. " +
      "Requires checkpointing enabled on slaves. Allows tasks to continue " +
      "running during mesos-slave restarts and upgrades",
    descrNo = "Disable checkpointing of tasks.",
    prefix = "disable_",
    noshort = true,
    default = Some(true))

  lazy val localPortMin = opt[Int](
    "local_port_min",
    descr = "Min port number to use when assigning globally unique service ports to apps.",
    default = Some(10000))

  lazy val localPortMax = opt[Int](
    "local_port_max",
    descr = "Max port number to use when assigning globally unique service ports to apps.",
    default = Some(20000))

  lazy val defaultExecutor = opt[String](
    "executor",
    descr = "Executor to use when none is specified. If not defined the Mesos command executor is used by default.",
    default = Some("//cmd"))

  lazy val hostname = opt[String](
    "hostname",
    descr = "The advertised hostname that is used for the communication with the Mesos master. " +
      "The value is also stored in the persistent store so another standby host can redirect to the elected leader.",
    default = Some(MarathonConfHostNameCache.hostname))

  lazy val webuiUrl = opt[String](
    "webui_url",
    descr = "The HTTP(S) url of the web ui, defaulting to the advertised hostname.",
    noshort = true,
    default = None)

  lazy val maxConcurrentHttpConnections = opt[Int](
    "http_max_concurrent_requests",
    descr = "The number of concurrent HTTP requests that are allowed before rejecting.",
    noshort = true,
    default = None
  )

  lazy val accessControlAllowOrigin: ScallopOption[Seq[String]] = opt[String](
    "access_control_allow_origin",
    descr = "The origin(s) to allow in Marathon. Not set by default. " +
      "Example values are \"*\", or " +
      "\"http://localhost:8888, http://domain.com\"",
    noshort = true,
    default = None).map(_.split(",").map(_.trim).toVector)

  def executor: Executor = Executor.dispatch(defaultExecutor())

  lazy val mesosRole = opt[Role](
    "mesos_role",
    descr = "Mesos role for this framework. " +
      "If set, Marathon receives resource offers for the specified role in addition to " +
      "resources with the role designation '*'.",
    default = Some(ResourceRole.Unreserved))

  lazy val newGroupEnforceRole = opt[NewGroupEnforceRoleBehavior](
    name = "new_group_enforce_role",
    descr = "Used to control whether role enforcement is enabled for new top-level groups; 'top' means that all new" +
      " top-level groups will have enforceRole enabled, and the contained services will be required to use the" +
      " group-role (a role that matches the top-level group's name).",
    default = Some(NewGroupEnforceRoleBehavior.Off))(groupRoleBehaviorConverter)

  @deprecated("Accepted resource roles can not be set directly anymore. Use accepted_resource_roles_default_behavior.", since = "1.9")
  private[this] lazy val defaultAcceptedResourceRoles = opt[String](
    "default_accepted_resource_roles",
    descr =
      "(Deprecated) Default for the defaultAcceptedResourceRoles attribute of all app definitions" +
        " as a comma-separated list of strings. " +
        "This parameter is deprecated, please use --accepted_resource_roles_default_behavior",
    default = None,
    validate = validateDefaultAcceptedResourceRoles).map(parseDefaultAcceptedResourceRoles)

  private[this] def parseDefaultAcceptedResourceRoles(str: String): Set[String] =
    str.split(',').map(_.trim).toSet

  private[this] def validateDefaultAcceptedResourceRoles(str: String): Boolean = {
    def expectedResourceRoles: Set[String] = mesosRole.toOption match {
      case Some(role) => Set(role, ResourceRole.Unreserved)
      case None => Set(ResourceRole.Unreserved)
    }

    val parsed = parseDefaultAcceptedResourceRoles(str)
    require(BuildInfo.version.minor < 10, "This flag has been removed in Marathon 1.10.0; use --accepted_resource_roles_default_behavior, instead.")

    // throw exceptions for better error messages
    require(parsed.nonEmpty, "--default_accepted_resource_roles must not be empty")
    require(
      parsed.forall(expectedResourceRoles),
      "--default_accepted_resource_roles contains roles for which we will not receive offers: " +
        (parsed -- expectedResourceRoles).mkString(", "))

    true
  }

  private[this] def deriveDefaultAcceptedResourceRolesDefaultBehavior: Option[AcceptedResourceRolesDefaultBehavior] = {
    defaultAcceptedResourceRoles.toOption match {
      case Some(set) if set == Set(ResourceRole.Unreserved) => Some(AcceptedResourceRolesDefaultBehavior.Unreserved)
      case Some(set) if set == Set(mesosRole()) => Some(AcceptedResourceRolesDefaultBehavior.Reserved)
      case _ => Some(AcceptedResourceRolesDefaultBehavior.Any)
    }
  }

  def defaultAcceptedResourceRolesSet(serviceRole: Role): Set[String] = {
    acceptedResourceRolesDefaultBehavior() match {
      case AcceptedResourceRolesDefaultBehavior.Any => Set(serviceRole, ResourceRole.Unreserved)
      case AcceptedResourceRolesDefaultBehavior.Unreserved => Set(ResourceRole.Unreserved)
      case AcceptedResourceRolesDefaultBehavior.Reserved => Set(serviceRole)
    }
  }

  lazy val acceptedResourceRolesDefaultBehavior: ScallopOption[AcceptedResourceRolesDefaultBehavior] = opt[AcceptedResourceRolesDefaultBehavior](
    name = "accepted_resource_roles_default_behavior",
    descr = "Default behavior for acceptedResourceRoles if not explicitly set on a service." +
      "This defaults to 'any', which allows either unreserved or reserved resources",
    validate = { _ =>
      require(!(defaultAcceptedResourceRoles.isSupplied && acceptedResourceRolesDefaultBehavior.isSupplied), "You may not specify both --default_accepted_resource_roles and --accepted_resource_roles_default_behavior")
      true
    },
    default = deriveDefaultAcceptedResourceRolesDefaultBehavior)(acceptedResourceRolesDefaultBehaviorConverter)

  lazy val gracefulShutdownTimeout = opt[Long] (
    "graceful_shutdown_timeout",
    descr = "Time, in milliseconds, to wait for a graceful Marathon shutdown on eg abdication.",
    default = Some(45000L)) // 45 seconds.

  lazy val taskLaunchConfirmTimeout = opt[Long](
    "task_launch_confirm_timeout",
    descr = "Time, in milliseconds, to wait for a task to enter " +
      "the TASK_STAGING state before killing it.",
    default = Some(300000L))

  lazy val taskLaunchTimeout = opt[Long](
    "task_launch_timeout",
    descr = "Time, in milliseconds, to wait for a task to enter " +
      "the TASK_RUNNING state before killing it.",
    default = Some(300000L)) // 300 seconds (5 minutes)

  lazy val taskReservationTimeout = opt[Long](
    "task_reservation_timeout",
    descr = "Time, in milliseconds, to wait for a new reservation to be acknowledged " +
      "via a received offer before deleting it.",
    default = Some(20000L)) // 20 seconds

  lazy val reconciliationInitialDelay = opt[Long](
    "reconciliation_initial_delay",
    descr = "This is the length of time, in milliseconds, before Marathon " +
      "begins to periodically perform task reconciliation operations",
    default = Some(15000L)) // 15 seconds

  lazy val reconciliationInterval = opt[Long](
    "reconciliation_interval",
    descr = "This is the length of time, in milliseconds, between task " +
      "reconciliation operations.",
    default = Some(600000L)) // 600 seconds (10 minutes)

  lazy val scaleAppsInitialDelay = opt[Long](
    "scale_apps_initial_delay",
    descr = "This is the length of time, in milliseconds, before Marathon " +
      "begins to periodically attempt to scale apps.",
    default = Some(15000L)) // 15 seconds

  lazy val scaleAppsInterval = opt[Long](
    "scale_apps_interval",
    descr = "This is the length of time, in milliseconds, between task " +
      "scale apps.",
    default = Some(300000L)) // 300 seconds (5 minutes)

  lazy val mesosUser = opt[String](
    "mesos_user",
    descr = "Mesos user for this framework.",
    default = new SystemProperties().get("user.name")) // Current logged in user

  lazy val frameworkName = opt[String](
    "framework_name",
    descr = "Framework name to register with Mesos.",
    default = Some("marathon"))

  lazy val mesosAuthentication = toggle(
    "mesos_authentication",
    default = Some(false),
    noshort = true,
    descrYes = "Will use framework authentication while registering with Mesos with principal and optional secret.",
    descrNo = "(Default) will not use framework authentication while registering with Mesos.",
    prefix = "disable_"
  )
  dependsOnAll(mesosAuthentication, List(mesosAuthenticationPrincipal))

  lazy val mesosAuthenticationPrincipal = opt[String](
    "mesos_authentication_principal",
    descr = "Mesos Authentication Principal.",
    noshort = true
  )

  lazy val mesosAuthenticationSecretFile = opt[String](
    "mesos_authentication_secret_file",
    descr = "Path to a file containing the Mesos Authentication Secret.",
    noshort = true
  )
  lazy val mesosAuthenticationSecret = opt[String](
    "mesos_authentication_secret",
    descr = "Mesos Authentication Secret.",
    noshort = true
  )
  mutuallyExclusive(mesosAuthenticationSecret, mesosAuthenticationSecretFile)

  lazy val envVarsPrefix = opt[String](
    "env_vars_prefix",
    descr = "Prefix to use for environment variables injected automatically into all started tasks.",
    noshort = true
  )

  //Internal settings, that are not intended for external use

  lazy val maxApps = opt[Int](
    "max_apps",
    descr = "The maximum number of applications that may be created.",
    noshort = true
  )

  lazy val onElectedPrepareTimeout = opt[Long](
    "on_elected_prepare_timeout",
    descr = "The timeout for preparing the Marathon instance when elected as leader.",
    default = Some(3 * 60 * 1000L) //3 minutes
  )

  lazy val leaderElectionBackend = opt[String](
    "leader_election_backend",
    descr = "The backend for leader election to use.",
    hidden = true,
    validate = Set("curator").contains,
    default = Some("curator")
  )

  lazy val mesosHeartbeatInterval = opt[Long](
    "mesos_heartbeat_interval",
    descr = "(milliseconds) in the absence of receiving a message from the mesos master " +
      "during a time window of this duration, attempt to coerce mesos into communicating with marathon.",
    noshort = true,
    hidden = true,
    default = Some(MesosHeartbeatMonitor.DEFAULT_HEARTBEAT_INTERVAL_MS))

  lazy val mesosHeartbeatFailureThreshold = opt[Int](
    "mesos_heartbeat_failure_threshold",
    descr = "after missing this number of expected communications from the mesos master, " +
      "infer that marathon has become disconnected from the master.",
    noshort = true,
    hidden = true,
    default = Some(MesosHeartbeatMonitor.DEFAULT_HEARTBEAT_FAILURE_THRESHOLD))

  lazy val drainingSeconds = opt[Long](
    "draining_seconds",
    descr = "(Default: 0 seconds) the seconds when marathon will start declining offers before a maintenance " +
      "window start time. This has no effect if '--disable_maintenance_mode' is specified!",
    default = Some(0)
  )

  lazy val maintenanceMode = toggle(
    "maintenance_mode",
    descrYes = "(Default) Marathon will decline offers from agents undergoing an active maintenance window",
    descrNo = "(Default) Marathon will ignore maintenance windows, accepting offers from agents undergoing an active maintenance window",
    prefix = "disable_",
    noshort = true,
    default = Some(true))

  lazy val gpuSchedulingBehavior = opt[GpuSchedulingBehavior](
    name = "gpu_scheduling_behavior",
    descr = "Defines how offered GPU resources should be treated. Has no effect without --enable_features gpu_resources. " +
      s"Possible settings are ${GpuSchedulingBehavior.all.map(_.name).mkString(", ")}",
    noshort = true,
    default = Some(GpuSchedulingBehavior.Restricted)
  )(gpuSchedulingBehaviorConverter)

  //  we must set gc actor threshold to not trigger it too early, 2 * max deployments looks like a good default
  validate(maxRunningDeployments, groupVersionsCacheSize) { (maxDeployments, versionsCacheSize) =>
    if (versionsCacheSize > 2 * maxDeployments) {
      Right(Unit)
    } else {
      Left(s"${groupVersionsCacheSize.name} must be more than 2 times higher than ${maxRunningDeployments.name}")
    }
  }

  lazy val maxConcurrentMarathonHealthChecks = opt[Int](
    name = "max_concurrent_marathon_health_checks",
    descr = "Defines maximum number of concurrent *Marathon* health checks (HTTP/S and TCP). Note that setting a big value" +
      "here will overload Marathon leading to internal timeouts and unstable behavior.",
    noshort = true,
    default = Some(256)
  )
}

object MarathonConf extends StrictLogging {
  sealed trait MesosMasterConnection {
    def unredactedConnectionString: String
    def redactedConnectionString: String
    override def toString() = redactedConnectionString
  }

  object MesosMasterConnection {
    case class Zk(zkUrl: ZookeeperConf.ZkUrl) extends MesosMasterConnection {
      override def unredactedConnectionString = zkUrl.unredactedConnectionString
      override def redactedConnectionString = zkUrl.toString()
    }
    case class Http(url: URL) extends MesosMasterConnection {
      override def unredactedConnectionString = url.toString
      /**
        * Credentials are not provided via the URL
        */
      override def redactedConnectionString = url.toString
    }
    /**
      * Describes a protocol-less connection string. Unfortunately, we did not strictly validate the Mesos master string
      * in the past.
      */
    case class Unspecified(string: String) extends MesosMasterConnection {
      override def unredactedConnectionString = string
      /**
        * Credentials are not provided via the URL
        */
      override def redactedConnectionString = string
    }

  }

  val groupRoleBehaviorConverter = new ValueConverter[NewGroupEnforceRoleBehavior] {
    val argType = org.rogach.scallop.ArgType.SINGLE

    override def parse(s: List[(String, List[String])]): Either[String, Option[NewGroupEnforceRoleBehavior]] = s match {
      case (_, enforceGroupRole :: Nil) :: Nil =>
        NewGroupEnforceRoleBehavior.fromString(enforceGroupRole) match {
          case o @ Some(_) => Right(o)
          case None => Left(s"Setting $enforceGroupRole is invalid. Valid settings are ${NewGroupEnforceRoleBehavior.all.map(_.name).mkString(", ")}")
        }
      case Nil =>
        Right(None)
      case other =>
        Left("Expected exactly one default enforce group role")
    }
  }

  val acceptedResourceRolesDefaultBehaviorConverter = new ValueConverter[AcceptedResourceRolesDefaultBehavior] {
    val argType = org.rogach.scallop.ArgType.SINGLE

    override def parse(s: List[(String, List[String])]): Either[String, Option[AcceptedResourceRolesDefaultBehavior]] = s match {
      case (_, acceptedResourceRolesDefaultBehavior :: Nil) :: Nil =>
        AcceptedResourceRolesDefaultBehavior.fromString(acceptedResourceRolesDefaultBehavior) match {
          case o @ Some(_) => Right(o)
          case None => Left(s"Setting $acceptedResourceRolesDefaultBehavior is invalid. Valid settings are ${AcceptedResourceRolesDefaultBehavior.all.map(_.name).mkString(", ")}")
        }
      case Nil =>
        Right(None)
      case other =>
        Left("Expected exactly one acceptedResourceRoles default behavior")
    }
  }

  val gpuSchedulingBehaviorConverter = new ValueConverter[GpuSchedulingBehavior] {
    val argType = org.rogach.scallop.ArgType.SINGLE

    override def parse(s: List[(String, List[String])]): Either[String, Option[GpuSchedulingBehavior]] = s match {
      case (_, schedulingBehaviorName :: Nil) :: Nil =>
        if ((schedulingBehaviorName == "undefined") && BuildInfo.version >= SemVer(1, 9, 0))
          throw new NotImplementedError("'--gpu_scheduling_behavior undefined' has been removed. Please set either restricted or unrestricted.")
        GpuSchedulingBehavior.all.find(_.name == schedulingBehaviorName).map { behavior =>
          Right(Some(behavior))
        } getOrElse {
          Left(s"Setting $schedulingBehaviorName is invalid. Valid settings are ${GpuSchedulingBehavior.all.map(_.name).mkString(", ")}")
        }
      case Nil =>
        Right(None)
      case other =>
        Left("Expected exactly one GpuSchedulingBehavior")
    }
  }

  val httpUrlValueConverter = new ValueConverter[URL] {
    val argType = org.rogach.scallop.ArgType.SINGLE

    def parse(s: List[(String, List[String])]): Either[String, Option[URL]] = s match {
      case (_, urlString :: Nil) :: Nil =>
        try Right(Some(new java.net.URL(urlString)))
        catch {
          case ex: IllegalArgumentException =>
            return Left(s"${urlString} is not a valid URL")
        }
      case Nil =>
        Right(None)
      case other =>
        Left("Expected exactly one url")
    }
  }

  val mesosMasterValueConverter = new ValueConverter[MesosMasterConnection] {
    val argType = org.rogach.scallop.ArgType.SINGLE

    private val httpLike = "(?i)(^https?://.+)$".r
    def parse(s: List[(String, List[String])]): Either[String, Option[MesosMasterConnection]] = s match {
      case (_, zkUrlString :: Nil) :: Nil if zkUrlString.take(5).equalsIgnoreCase("zk://") =>
        ZookeeperConf.ZkUrl.parse(zkUrlString).map { zkUrl => Some(MesosMasterConnection.Zk(zkUrl)) }
      case (_, httpLike(httpUrlString) :: Nil) :: Nil =>
        httpUrlValueConverter.parse(s).map { url => url.map(MesosMasterConnection.Http(_)) }
      case (_, addressPort :: Nil) :: Nil =>
        // localhost:5050 or 127.0.0.1:5050 or leader.mesos:5050
        logger.warn(s"Specifying a Mesos Master connection without a protocol is deprecated and will likely be prohibited in the future. Please specify a protocol (http://, zk://, https://)")
        Right(Some(MesosMasterConnection.Unspecified(addressPort)))
      case Nil =>
        Right(None)
      case _ =>
        Left("Expected exactly one connection string")
    }
  }
}
