package mesosphere.mesos

import java.time.Clock

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.GpuSchedulingBehavior
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launcher.impl.TaskLabels
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.plugin.scheduler.SchedulerPlugin
import mesosphere.marathon.state._
import mesosphere.marathon.stream.Implicits._
import mesosphere.marathon.tasks.{OfferUtil, PortsMatch, PortsMatcher, ResourceUtil}
import mesosphere.mesos.protos.{Resource, ResourceProviderID}
import org.apache.mesos.Protos
import org.apache.mesos.Protos.Offer
import org.apache.mesos.Protos.Resource.DiskInfo.Source
import mesosphere.marathon.silent

import scala.annotation.tailrec
import scala.collection.immutable.Seq

object ResourceMatcher extends StrictLogging {
  import ResourceUtil.RichResource
  type Role = String

  /**
    * A successful match result of the [[ResourceMatcher]].matchResources method.
    */
  case class ResourceMatch(scalarMatches: Seq[ScalarMatch], portsMatch: PortsMatch) {
    lazy val hostPorts: Seq[Option[Int]] = portsMatch.hostPorts

    def scalarMatch(name: String): Option[ScalarMatch] = scalarMatches.find(_.resourceName == name)

    def resources: Seq[Protos.Resource] =
      scalarMatches.flatMap(_.consumedResources)(collection.breakOut) ++
        portsMatch.resources

    // TODO - this assumes that volume matches are one resource to one volume, which should be correct, but may not be.
    val localVolumes: Seq[DiskResourceMatch.ConsumedVolume] =
      scalarMatches.collect { case r: DiskResourceMatch => r.volumes }.flatten
  }

  /**
    * Restricts which resources are considered for matching.
    *
    * Disk resources are always discarded, since we do not want to match them by
    * accident.
    *
    * @param acceptedRoles contains all Mesos resource roles that are accepted
    * @param needToReserve if true, only unreserved resources will considered
    * @param labelMatcher a matcher that checks if the given resource labels
    *                     are compliant with the expected or not expected labels
    */
  case class ResourceSelector(
      acceptedRoles: Set[String],
      needToReserve: Boolean,
      labelMatcher: LabelMatcher) {

    def apply(resource: Protos.Resource): Boolean = {
      import ResourceSelector._
      // resources with disks are matched by the VolumeMatcher or not at all
      val noAssociatedVolume = !(resource.hasDisk && resource.getDisk.hasVolume)
      def matchesLabels: Boolean = labelMatcher.matches(reservationLabels(resource))

      noAssociatedVolume && acceptedRoles(resource.getRole: @silent) && matchesLabels
    }

    override def toString: String = {
      val reserveString = if (needToReserve) " to reserve" else ""
      val rolesString = acceptedRoles.mkString(", ")
      s"Considering resources$reserveString with roles {$rolesString} $labelMatcher"
    }
  }

  object ResourceSelector {
    /** The reservation labels if the resource is reserved, or an empty Map */
    private def reservationLabels(resource: Protos.Resource): Map[String, String] =
      if (!resource.hasReservation || !resource.getReservation.hasLabels)
        Map.empty
      else {
        resource.getReservation.getLabels.getLabelsList.map { label =>
          label.getKey -> label.getValue
        }(collection.breakOut)
      }

    /** Match resources with given roles that have at least the given labels */
    def reservedWithLabels(acceptedRoles: Set[String], labels: Map[String, String]): ResourceSelector = {
      ResourceSelector(acceptedRoles, needToReserve = false, LabelMatcher.WithReservationLabels(labels))
    }
    /** Match resources with given roles that do not have known reservation labels */
    def reservable: ResourceSelector = {
      ResourceSelector(Set(ResourceRole.Unreserved), needToReserve = true, LabelMatcher.WithoutReservationLabels)
    }

    /** Match any resources with given roles that do not have known reservation labels */
    def any(acceptedRoles: Set[String]): ResourceSelector = {
      ResourceSelector(acceptedRoles, needToReserve = false, LabelMatcher.WithoutReservationLabels)
    }
  }

  private[mesos] sealed trait LabelMatcher {
    def matches(resourceLabels: Map[String, String]): Boolean
  }

  private[this] object LabelMatcher {
    case class WithReservationLabels(labels: Map[String, String]) extends LabelMatcher {
      override def matches(resourceLabels: Map[String, String]): Boolean =
        labels.forall { case (k, v) => resourceLabels.get(k).contains(v) }

      override def toString: Role = {
        val labelsStr = labels.map { case (k, v) => s"$k: $v" }.mkString(", ")
        s"and labels {$labelsStr}"
      }
    }

    case object WithoutReservationLabels extends LabelMatcher {
      override def matches(resourceLabels: Map[String, String]): Boolean =
        resourceLabels.keys.toSet.intersect(TaskLabels.labelKeysForReservations).isEmpty

      override def toString: Role = "without resident reservation labels"
    }
  }

  /**
    * Checks whether the given offer contains enough resources to launch a task of the given run spec
    * or to make a reservation for a task.
    *
    * If a task uses local volumes, this method is typically called twice for every launch. Once
    * for the reservation on UNRESERVED resources and once for every (re-)launch on RESERVED resources.
    *
    * If matching on RESERVED resources as specified by the ResourceSelector, resources for volumes
    * have to be matched separately (e.g. by the [[PersistentVolumeMatcher]]). If matching on UNRESERVED
    * resources, the disk resources for the local volumes are included since they must become part of
    * the reservation.
    */
  def matchResources(
    offer: Offer,
    runSpec: RunSpec,
    knownInstances: => Seq[Instance],
    selector: ResourceSelector,
    conf: MatcherConf,
    schedulerPlugins: Seq[SchedulerPlugin],
    localRegion: Option[Region] = None,
    reservedInstances: Seq[Instance] = Seq.empty)(implicit clock: Clock): ResourceMatchResponse = {

    val groupedResources: Map[Role, Seq[Protos.Resource]] = offer.getResourcesList.groupBy(_.getName).map { case (k, v) => k -> v.to[Seq] }

    val scalarResourceMatch = matchScalarResource(groupedResources, selector) _
    val diskResourceMatch = matchDiskResource(groupedResources, selector) _

    // Local volumes only need to be matched if we are making a reservation for resident tasks --
    // that means if the resources that are matched are still unreserved.
    def needToReserveDisk = selector.needToReserve && runSpec.diskForPersistentVolumes > 0

    val diskMatch = if (needToReserveDisk) {
      val volumes = runSpec.persistentVolumes
      val mounts = runSpec.persistentVolumeMounts
      val volumesWithMounts = runSpec match {
        case _: AppDefinition =>
          volumes.zip(mounts).map {
            case (volume, mount) => VolumeWithMount(volume, mount)
          }
        case _: PodDefinition =>
          volumes.map { volume =>
            val mountPath = volume.name.getOrElse(throw new IllegalStateException("Failed to retrieve a volume name"))
            val mount = VolumeMount(volume.name, mountPath, readOnly = false)
            VolumeWithMount(volume, mount)
          }
      }
      diskResourceMatch(
        runSpec.resources.disk,
        volumesWithMounts,
        ScalarMatchResult.Scope.IncludingLocalVolumes)
    } else {
      diskResourceMatch(runSpec.resources.disk, Nil, ScalarMatchResult.Scope.ExcludingLocalVolumes)
    }

    val scalarMatchResults = (
      Seq(
        scalarResourceMatch(Resource.CPUS, runSpec.resources.cpus, ScalarMatchResult.Scope.NoneDisk),
        scalarResourceMatch(Resource.MEM, runSpec.resources.mem, ScalarMatchResult.Scope.NoneDisk),
        scalarResourceMatch(Resource.GPUS, runSpec.resources.gpus.toDouble, ScalarMatchResult.Scope.NoneDisk),
        scalarResourceMatch(Resource.NETWORK_BANDWIDTH, runSpec.resources.networkBandwidth.toDouble, ScalarMatchResult.Scope.NoneDisk)) ++
        diskMatch
    ).filter(_.requiredValue != 0)

    // add scalar resources to noOfferMatchReasons
    val noOfferMatchReasons = scalarMatchResults
      .filter(scalar => !scalar.matches)
      .map(scalar => NoOfferMatchReason.fromResourceType(scalar.resourceName)).toBuffer

    var onMatchActions: Vector[() => Unit] = Vector.empty
    def addOnMatch(action: () => Unit) = {
      onMatchActions :+= action
    }

    // Current mesos implementation will only send resources with one distinct role assigned.
    // If not a single resource (matching the resource selector) was found, a NoOfferMatchReason.UnmatchedRole
    // will be added to noOfferMatchReasons
    if (!offer.getResourcesList.exists(resource => selector.apply(resource))) {
      noOfferMatchReasons += NoOfferMatchReason.UnfulfilledRole
    }

    logUnsatisfiedResources(offer, selector, scalarMatchResults)

    def portsMatchOpt: Option[PortsMatch] = PortsMatcher(runSpec, offer, selector).portsMatch

    val meetsFaultDomainRequirements: Boolean = {
      val faultDomainFields = Set(Constraints.regionField, Constraints.zoneField)
      val offerHasFaultDomainConstraints = runSpec.constraints.exists(c => faultDomainFields.contains(c.getField))
      val maybeOfferRegion = OfferUtil.region(offer)
      val offerIsFromLocalRegion = maybeOfferRegion.isEmpty || localRegion.exists(region => maybeOfferRegion.contains(region.value))
      offerHasFaultDomainConstraints || offerIsFromLocalRegion
    }

    val meetsAllConstraints: Boolean = {
      lazy val instances = knownInstances.filter { inst =>
        // we ignore instances of older configurations: this way we can place a new instance on an agent that already
        // hosts an old instance that will be killed once a new one is running/healthy/ready
        inst.runSpecVersion >= runSpec.versionInfo.lastConfigChangeVersion
      }
      val badConstraints = runSpec.constraints.filterNot { constraint =>
        Constraints.meetsConstraint(instances, offer, constraint)
      }

      if (badConstraints.nonEmpty) {
        // Add constraints to noOfferMatchReasons
        noOfferMatchReasons += NoOfferMatchReason.UnfulfilledConstraint
        logger.info(
          s"Offer [${offer.getId.getValue}] with role [${offer.getAllocationInfo.getRole}]. Constraints for run spec [${runSpec.id}] not satisfied." +
            s"The conflicting constraints are: [${badConstraints.mkString(", ").replaceAll("\n", " ")}]. Agent is ${offer.getHostname}"
        )
      }

      badConstraints.isEmpty
    }

    val checkAvailability: Boolean = {
      logger.info(s"Studiying offer [${offer.getId.getValue}] availability: ${offer.getHostname}\n")
      if (conf.maintenanceMode()) {
        logger.trace(s"Studiying offer [${offer.getId.getValue}] availability: maintenance feature activated\n")
        val result = Availability.offerAvailable(offer, conf.drainingTime)
        if (!result) {
          logger.trace(s"Studiying offer [${offer.getId.getValue}] availability: offer is not available\n")
          noOfferMatchReasons += NoOfferMatchReason.UnfulfilledConstraint
          // Add unavailability to noOfferMatchReasons
          noOfferMatchReasons += NoOfferMatchReason.AgentMaintenance
          logger.info(s"Offer [${offer.getId.getValue}]. Agent [${offer.getSlaveId}] on host [${offer.getHostname}] unavailable.\n")
        }
        result
      } else true
    }

    val checkGpuSchedulingBehaviour: Boolean = {
      val applicationSpecificGpuBehavior: Option[GpuSchedulingBehavior] = runSpec.labels.get("GPU_SCHEDULING_BEHAVIOR").flatMap { behaviorName =>
        validBehaviors.find(_.name == behaviorName)
      }
      val availableGPUs = groupedResources.getOrElse(Resource.GPUS, Nil).foldLeft(0.0)(_ + _.getScalar.getValue)
      val gpuResourcesAreWasted = availableGPUs > 0 && runSpec.resources.gpus == 0
      applicationSpecificGpuBehavior.getOrElse(conf.gpuSchedulingBehavior()) match {
        case GpuSchedulingBehavior.Restricted =>
          val noPersistentVolumeToMatch = PersistentVolumeMatcher.matchVolumes(offer, reservedInstances).isEmpty
          if (!gpuResourcesAreWasted) {
            true
          } else if (gpuResourcesAreWasted && noPersistentVolumeToMatch) {
            noOfferMatchReasons += NoOfferMatchReason.DeclinedScarceResources
            false
          } else {
            addOnMatch(() => logger.info(s"Runspec [${runSpec.id}] doesn't require any GPU resources but " +
              "will be launched on an agent with GPU resources due to required persistent volume."))
            true
          }

        case GpuSchedulingBehavior.Unrestricted =>
          true
      }
    }

    val resourceMatchOpt = if (scalarMatchResults.forall(_.matches)
      && meetsFaultDomainRequirements
      && meetsAllConstraints
      && checkAvailability
      && schedulerPlugins.forall(_.isMatch(offer, runSpec))
      && checkGpuSchedulingBehaviour) {
      portsMatchOpt match {
        case Some(portsMatch) =>
          onMatchActions.foreach(action => action.apply())
          Some(ResourceMatch(scalarMatchResults.collect { case m: ScalarMatch => m }, portsMatch))
        case None =>
          // Add ports to noOfferMatchReasons
          noOfferMatchReasons += NoOfferMatchReason.InsufficientPorts
          None
      }
    } else {
      None
    }

    resourceMatchOpt match {
      case Some(resourceMatch) => ResourceMatchResponse.Match(resourceMatch)
      case None => ResourceMatchResponse.NoMatch(noOfferMatchReasons.to[Seq])
    }
  }

  private[mesos] case class SourceResources(source: Option[Source], resources: List[Protos.Resource]) {
    lazy val size = resources.foldLeft(0.0)(_ + _.getScalar.getValue)
  }
  private[mesos] object SourceResources extends ((Option[Source], List[Protos.Resource]) => SourceResources) {
    def listFromResources(l: List[Protos.Resource]): List[SourceResources] = {
      l.groupBy(_.getDiskSourceOption).map(SourceResources.tupled).toList
    }
  }

  /*
   Prioritize resources to make the most sensible allocation.
   - If requesting full disk, allocate the smallest disk volume that meets constraints
   - If requesting root, just return it because there can only be one.
   - If requesting path disk, allocate the largest volume possible to spread allocation evenly.
   This may not be ideal if you'd prefer to leave room for larger allocations instead.

   TODO - test. Also, parameterize?
   */
  private[this] def prioritizeDiskResources(
    diskType: DiskType,
    resources: List[SourceResources]): List[SourceResources] = {
    diskType match {
      case DiskType.Root =>
        resources
      case DiskType.Path =>
        resources.sortBy(_.size)(implicitly[Ordering[Double]].reverse)
      case DiskType.Mount =>
        resources.sortBy(_.size)
    }
  }

  // format: OFF
  @tailrec
  private[this] def findDiskGroupMatches(
    requiredValue: Double,
    resourcesRemaining: List[SourceResources],
    allSourceResources: List[SourceResources],
    matcher: Protos.Resource => Boolean):
      Option[(Option[Source], List[GeneralScalarMatch.Consumption], List[SourceResources])] =
    // format: ON
    resourcesRemaining match {
      case Nil =>
        None
      case next :: rest =>
        consumeResources(requiredValue, next.resources, matcher = matcher) match {
          case Left(_) =>
            findDiskGroupMatches(requiredValue, rest, allSourceResources, matcher)
          case Right((resourcesConsumed, remainingAfterConsumption)) =>
            val sourceResourcesAfterConsumption = if (remainingAfterConsumption.isEmpty)
              None
            else
              Some(next.copy(resources = remainingAfterConsumption))

            Some((
              next.source,
              resourcesConsumed,
              (sourceResourcesAfterConsumption ++ allSourceResources.filterNot(_ == next)).toList))
        }
    }

  /**
    * Match volumes against disk resources and return results which keep disk sources and persistentVolumes associated.
    *
    * TODO - handle matches for a single volume across multiple resource offers for the same disk
    */
  private[this] def matchDiskResource(
    groupedResources: Map[Role, Seq[Protos.Resource]], selector: ResourceSelector)(
    scratchDisk: Double,
    volumesWithMounts: Seq[VolumeWithMount[PersistentVolume]],
    scope: ScalarMatchResult.Scope): Seq[ScalarMatchResult] = {

    @tailrec
    def findMatches(
      diskType: DiskType,
      pendingAllocations: List[Either[Double, VolumeWithMount[PersistentVolume]]],
      resourcesRemaining: List[SourceResources],
      resourcesConsumed: List[DiskResourceMatch.Consumption] = Nil): Either[DiskResourceNoMatch, DiskResourceMatch] = {
      val orderedResources = prioritizeDiskResources(diskType, resourcesRemaining)

      pendingAllocations match {
        case Nil =>
          Right(DiskResourceMatch(diskType, resourcesConsumed, scope))
        case nextAllocation :: restAllocations =>
          val (matcher, nextAllocationSize) = nextAllocation match {
            case Left(size) => ({ _: Protos.Resource => true }, size)
            case Right(VolumeWithMount(volume, _)) =>
              def matcher(resource: Protos.Resource): Boolean = {
                VolumeConstraints.meetsAllConstraints(resource, volume.persistent.constraints) &&
                  VolumeProfileMatcher.matchesProfileName(volume.persistent.profileName, resource)
              }

              (matcher _, volume.persistent.size.toDouble)
          }

          findDiskGroupMatches(nextAllocationSize, orderedResources, orderedResources, matcher) match {
            case None =>
              Left(
                DiskResourceNoMatch(resourcesConsumed, resourcesRemaining.flatMap(_.resources), nextAllocation, scope))
            case Some((source, generalConsumptions, decrementedResources)) =>
              val consumptions = generalConsumptions.map { c =>
                val volumeWithMount = nextAllocation.right.toOption
                DiskResourceMatch.Consumption(c, source, volumeWithMount)
              }

              findMatches(
                diskType,
                restAllocations,
                decrementedResources,
                consumptions ++ resourcesConsumed)
          }
      }
    }

    /*
      * The implementation for finding mount matches differs from disk matches because:
      * - A mount volume cannot be partially allocated. The resource allocation request must be sized up to match the
      *   actual resource size
      * - The mount volume can't be split amongst reserved / non-reserved.
      * - The mount volume has an extra maxSize concern
      *
      * If this method can be generalized to worth with the above code, then so be it.
      */
    @tailrec
    def findMountMatches(
      pendingAllocations: List[VolumeWithMount[PersistentVolume]],
      resources: List[Protos.Resource],
      resourcesConsumed: List[DiskResourceMatch.Consumption] = Nil): Either[DiskResourceNoMatch, DiskResourceMatch] = {
      pendingAllocations match {
        case Nil =>
          Right(DiskResourceMatch(DiskType.Mount, resourcesConsumed, scope))
        case nextAllocation :: restAllocations =>
          resources.find { resource =>
            val resourceSize = resource.getScalar.getValue
            VolumeConstraints.meetsAllConstraints(resource, nextAllocation.volume.persistent.constraints) &&
              (resourceSize >= nextAllocation.volume.persistent.size) &&
              (resourceSize <= nextAllocation.volume.persistent.maxSize.getOrElse(Long.MaxValue)) &&
              VolumeProfileMatcher.matchesProfileName(nextAllocation.volume.persistent.profileName, resource)
          } match {
            case Some(matchedResource) =>
              val consumedAmount = matchedResource.getScalar.getValue
              val grownVolumeWithMount = nextAllocation.copy(volume = nextAllocation.volume.copy(
                persistent = nextAllocation.volume.persistent.copy(size = consumedAmount.toLong)))
              val consumption =
                DiskResourceMatch.Consumption(
                  consumedAmount,
                  role = matchedResource.getRole: @silent,
                  providerId = if (matchedResource.hasProviderId) Option(ResourceProviderID(matchedResource.getProviderId.getValue)) else None,
                  reservation = if (matchedResource.hasReservation) Option(matchedResource.getReservation) else None,
                  source = DiskSource.fromMesos(matchedResource.getDiskSourceOption),
                  Some(grownVolumeWithMount))

              findMountMatches(
                restAllocations,
                resources.filterNot(_ == matchedResource),
                consumption :: resourcesConsumed)
            case None =>
              Left(DiskResourceNoMatch(resourcesConsumed, resources, Right(nextAllocation), scope))
          }
      }
    }

    val diskResources = groupedResources.getOrElse(Resource.DISK, Seq.empty)

    val withoutUnsupportedTypes = diskResources.filterNot { r =>
      r.getDiskSourceOption.exists { source =>
        source.getType == Source.Type.BLOCK ||
          source.getType == Source.Type.RAW
      }
    }

    val resourcesByType: Map[DiskType, Seq[Protos.Resource]] = withoutUnsupportedTypes.groupBy { r =>
      DiskSource.fromMesos(r.getDiskSourceOption).diskType
    }.withDefault(_ => Nil)

    val scratchDiskRequest = if (scratchDisk > 0.0) Some(Left(scratchDisk)) else None
    val requestedResourcesByType: Map[DiskType, Seq[Either[Double, VolumeWithMount[PersistentVolume]]]] =
      (scratchDiskRequest ++ volumesWithMounts.map(Right(_)).toList).groupBy {
        case Left(_) => DiskType.Root
        case Right(vm) => vm.volume.persistent.`type`
      }.map { case (k, v) => k -> v.to[Seq] }

    requestedResourcesByType.keys.map { diskType =>
      val withBiggestRequestsFirst =
        requestedResourcesByType(diskType).
          toList.
          sortBy({ r => r.right.map(_.volume.persistent.size.toDouble).merge })(implicitly[Ordering[Double]].reverse)

      val resources: List[Protos.Resource] = resourcesByType(diskType).filterAs(selector(_))(collection.breakOut)

      if (diskType == DiskType.Mount) {
        findMountMatches(
          withBiggestRequestsFirst.flatMap(_.right.toOption),
          resources)
      } else
        findMatches(
          diskType,
          withBiggestRequestsFirst,
          SourceResources.listFromResources(resources))
    }.toList.map(_.merge)
  }

  private[this] def matchScalarResource(
    groupedResources: Map[Role, Seq[Protos.Resource]], selector: ResourceSelector)(
    name: String, requiredValue: Double,
    scope: ScalarMatchResult.Scope): ScalarMatchResult = {

    require(scope == ScalarMatchResult.Scope.NoneDisk || name == Resource.DISK)

    val resourcesForName = groupedResources.getOrElse(name, Seq.empty)
    val matchingScalarResources = resourcesForName.filter(selector(_))
    consumeResources(requiredValue, matchingScalarResources.toList) match {
      case Left(valueLeft) =>
        NoMatch(name, requiredValue, requiredValue - valueLeft, scope = scope)
      case Right((resourcesConsumed, remaining)) =>
        GeneralScalarMatch(name, requiredValue, resourcesConsumed, scope = scope)
    }
  }

  /**
    * Given a list of resources, allocates the specified size.
    *
    * Returns an either:
    *
    * - Left:  indicates failure; contains the amount failed to be matched.
    * - Right: indicates success; contains a list of consumptions and a list of resources remaining after the
    *     allocation.
    */
  @tailrec
  private[this] def consumeResources(
    valueLeft: Double,
    resourcesLeft: List[Protos.Resource],
    resourcesNotConsumed: List[Protos.Resource] = Nil,
    resourcesConsumed: List[GeneralScalarMatch.Consumption] = Nil,
    matcher: Protos.Resource => Boolean = { _ => true }):
      // format: OFF
      Either[(Double), (List[GeneralScalarMatch.Consumption], List[Protos.Resource])] = {
    // format: ON
    if (valueLeft <= 0) {
      Right((resourcesConsumed, resourcesLeft ++ resourcesNotConsumed))
    } else {
      resourcesLeft match {
        case Nil => Left(valueLeft)

        case nextResource :: restResources =>
          if (matcher(nextResource)) {
            val consume = Math.min(valueLeft, nextResource.getScalar.getValue)
            val decrementedResource = ResourceUtil.consumeScalarResource(nextResource, consume)
            val newValueLeft = valueLeft - consume
            val providerId = if (nextResource.hasProviderId) Option(ResourceProviderID(nextResource.getProviderId.getValue)) else None
            val reservation = if (nextResource.hasReservation) Option(nextResource.getReservation) else None
            val consumedValue = GeneralScalarMatch.Consumption(consume, nextResource.getRole: @silent, providerId, reservation)

            consumeResources(newValueLeft, restResources, (decrementedResource ++ resourcesNotConsumed).toList,
              consumedValue :: resourcesConsumed, matcher)
          } else {
            consumeResources(valueLeft, restResources, nextResource :: resourcesNotConsumed, resourcesConsumed, matcher)
          }
      }
    }
  }

  private[this] def logUnsatisfiedResources(
    offer: Offer,
    selector: ResourceSelector,
    scalarMatchResults: Seq[ScalarMatchResult]): Unit = {
    if (scalarMatchResults.exists(!_.matches)) {
      val basicResourceString = scalarMatchResults.mkString(", ")
      logger.info(
        s"Offer [${offer.getId.getValue}] with role ${offer.getAllocationInfo.getRole}. " +
          s"$selector. " +
          s"Not all basic resources satisfied: $basicResourceString")
    }
  }

  private val validBehaviors = Set(GpuSchedulingBehavior.Restricted, GpuSchedulingBehavior.Unrestricted)
}
