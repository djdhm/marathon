package mesosphere.marathon
package core.deployment.impl

import akka.Done
import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props, UnhandledMessage}
import akka.stream.scaladsl.Source
import akka.testkit.TestActorRef
import mesosphere.AkkaUnitTest
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.condition.Condition.{Killed, Running}
import mesosphere.marathon.core.deployment.{DeploymentPlan, DeploymentStep}
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.health.{MarathonHttpHealthCheck, PortReference, Health, Healthy, Unhealthy}
import mesosphere.marathon.core.instance.Instance.InstanceState
import mesosphere.marathon.core.instance.update.InstanceChangedEventsGenerator
import mesosphere.marathon.core.instance.{Goal, GoalChangeReason, Instance, TestInstanceBuilder}
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.{ReadinessCheck, ReadinessCheckExecutor, ReadinessCheckResult}
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.metrics.dummy.DummyMetrics
import mesosphere.marathon.raml.Raml
import mesosphere.marathon.state._
import mesosphere.marathon.util.CancellableOnce
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.concurrent.{Future, Promise}

class TaskReplaceActorTest extends AkkaUnitTest with Eventually {
  "TaskReplaceActor" should {
    "replace old tasks without health checks" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 5,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        upgradeStrategy = UpgradeStrategy(0.0))
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 5) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 5, 0)
      eventually {
        for (instance <- oldInstances)
          verify(f.tracker).setGoal(instance.instanceId, Goal.Decommissioned, GoalChangeReason.Upgrading)
      }
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 5)

      promise.future.futureValue
      expectTerminated(ref)
    }

    "not kill new and already started tasks" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 5,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        upgradeStrategy = UpgradeStrategy(0.0))
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 4)
      eventually {
        verify(f.tracker).setGoal(oldInstances(0).instanceId, Goal.Decommissioned, GoalChangeReason.Upgrading)
        verify(f.tracker, never).setGoal(newInstances(0).instanceId, Goal.Decommissioned, GoalChangeReason.Upgrading)
      }
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 5)

      promise.future.futureValue
      expectTerminated(ref)
    }

    "replace old tasks with health checks" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 5,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(0.0))
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 5) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 5, 0)
      eventually {
        for (instance <- oldInstances)
          verify(f.tracker).setGoal(instance.instanceId, Goal.Decommissioned, GoalChangeReason.Upgrading)
      }
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 5)

      promise.future.futureValue
      verify(f.queue).resetDelay(newApp)

      expectTerminated(ref)
    }

    "replace and scale down from more than new minCapacity" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 3,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1.0))
      val newApp = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 2,
        versionInfo = VersionInfo.forNewConfig(Timestamp(1)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1.0))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 0)
      eventually {
        verify(f.tracker, once).setGoal(any, any, any)
        verify(f.queue, times(1)).add(newApp, 1)
      }

      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 1)
      eventually {
        verify(f.tracker, times(2)).setGoal(any, any, any)
        verify(f.queue, times(2)).add(newApp, 1)
      }

      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 2)
      eventually {
        verify(f.tracker, times(3)).setGoal(any, any, any)
      }

      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 2)
      promise.future.futureValue

      expectTerminated(ref)
    }

    "replace tasks with minimum running number of tasks" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 3,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(0.5)
      )
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 3) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 0)
      // all new tasks are queued directly
      eventually {
        verify(f.queue, times(1)).add(newApp, 3)
      }

      // ceiling(minimumHealthCapacity * 3) = 2 are left running
      eventually{
        verify(f.tracker, once).setGoal(any, any, any)
      }

      // first new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 1, newUnhealthy = 2)
      eventually {
        verify(f.tracker, times(2)).setGoal(any, any, any)
      }

      // second new task becomes healthy and the last old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 2, newUnhealthy = 1)
      eventually {
        verify(f.tracker, times(3)).setGoal(any, any, any)
      }

      // third new task becomes healthy
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 3)

      promise.future.futureValue
      expectTerminated(ref)
    }

    "respect upgrade strategy" in {
      Given("An app with health checks, 1 task of new version already started but not passing health checks yet")
      val f = new Fixture

      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 2,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(0.50, 0)
      )
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      // deployment should not complete within 1s, that's also a good way to wait for 1s to check for next assertion
      assert(promise.future.isReadyWithin(timeout = Span(1000, Millis)) == false)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 0, newUnhealthy = 1)
      // that's the core of this test: we haven't replaced task yet, see MARATHON-8716
      for (instance <- oldInstances)
        verify(f.tracker, never).setGoal(instance.instanceId, Goal.Decommissioned, GoalChangeReason.Upgrading)

      // we can now make this new instance healthy
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 1)
      // and we can check deployment continue as usual
      eventually {
        verify(f.tracker, times(1)).setGoal(any, any, any)
      }
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 1)
      eventually {
        verify(f.queue, times(1)).add(newApp, 1)
      }
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 2)
      promise.future.futureValue
      expectTerminated(ref)
    }

    "replace tasks during rolling upgrade *without* over-capacity" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 3,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(0.5, 0.0)
      )
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      // only one task is queued directly
      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 0)
      val queueOrder = org.mockito.Mockito.inOrder(f.queue)
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      // ceiling(minimumHealthCapacity * 3) = 2 are left running
      eventually {
        verify(f.tracker, once).setGoal(any, any, any)
      }

      // first new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 1)
      eventually {
        verify(f.tracker, times(2)).setGoal(any, any, any)
      }
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      // second new task becomes healthy and the last old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 2)
      eventually {
        verify(f.tracker, times(3)).setGoal(any, any, any)
      }
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      // third new task becomes healthy
      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 3)

      promise.future.futureValue
      expectTerminated(ref)
    }

    "replace tasks during rolling upgrade *with* minimal over-capacity" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 3,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(1.0, 0.0) // 1 task over-capacity is ok
      )
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      // only one task is queued directly, all old still running
      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 0)
      val queueOrder = org.mockito.Mockito.inOrder(f.queue)
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
        verify(f.tracker, never).setGoal(any, any, any)
      }

      // first new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 1)
      eventually {
        verify(f.tracker, once).setGoal(any, any, any)
      }
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      // second new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 2)
      eventually {
        verify(f.tracker, times(2)).setGoal(any, any, any)
      }
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      // third new task becomes healthy and last old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 3)
      eventually {
        verify(f.tracker, times(3)).setGoal(any, any, any)
      }
      queueOrder.verify(f.queue, never).add(_: AppDefinition, 1)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 3)

      promise.future.futureValue
      expectTerminated(ref)
    }

    "replace tasks during rolling upgrade with 2/3 over-capacity" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 3,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(1.0, 0.7)
      )
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(eq(newApp), any) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      // two tasks are queued directly, all old still running
      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 0)
      val queueOrder = org.mockito.Mockito.inOrder(f.queue)
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 2)
      }

      // first new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 1, newUnhealthy = 1)
      eventually {
        verify(f.tracker, once).setGoal(any, any, any)
      }
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      // second new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 2, newUnhealthy = 1)
      eventually {
        verify(f.tracker, times(2)).setGoal(any, any, any)
      }
      queueOrder.verify(f.queue, never).add(_: AppDefinition, 1)

      // third new task becomes healthy and last old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 3)
      eventually {
        verify(f.tracker, times(3)).setGoal(any, any, any)
      }
      queueOrder.verify(f.queue, never).add(_: AppDefinition, 1)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 3)

      promise.future.futureValue
      expectTerminated(ref)
    }

    "downscale tasks during rolling upgrade with 1 over-capacity" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 4,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1.0, maximumOverCapacity = 0.3)
      )
      val newApp = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 3,
        versionInfo = VersionInfo.forNewConfig(Timestamp(1)),
        healthChecks = Set(MarathonHttpHealthCheck(portIndex = Some(PortReference(0)))),
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1.0, maximumOverCapacity = 0.3)
      )

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 4, 0)
      // one task is killed directly because we are over capacity
      eventually {
        verify(f.tracker).setGoal(any, eq(Goal.Decommissioned), eq(GoalChangeReason.Upgrading))
        verify(f.queue, times(1)).resetDelay(newApp)
      }

      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 0)
      eventually {
        verify(f.queue, times(1)).add(newApp, 1)
      }

      // first new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 3, 1)
      eventually {
        verify(f.tracker, times(2)).setGoal(any, any, any)
      }

      // Task is killed, and now we can schedule a new one
      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 1)
      eventually {
        verify(f.queue, times(2)).add(newApp, 1)
      }

      // second new task becomes healthy and another old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 2, 2)
      eventually {
        verify(f.tracker, times(3)).setGoal(any, any, any)
      }

      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 2)
      eventually {
        verify(f.queue, times(3)).add(newApp, 1)
      }

      // third new task becomes healthy and last old task is killed
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 3)
      eventually {
        verify(f.tracker, times(4)).setGoal(any, any, any)
      }

      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 3)
      promise.future.futureValue
      expectTerminated(ref)
    }

    "stop the actor if all tasks are replaced already" in {
      Given("An app without health checks and readiness checks, as well as 2 tasks of this version")
      val f = new Fixture
      val app = AppDefinition(id = AbsolutePathId("/myApp"), instances = 2, role = "*")
      val i = f.createInstances(app)
      val promise = Promise[Unit]()

      When("The replace actor is started")
      val ref = f.replaceActor(app, promise)
      watch(ref)
      f.sendState(app, app, ref, i, i, 0, 2)

      Then("The replace actor finishes immediately")
      promise.future.futureValue
      expectTerminated(ref)
    }

    "wait for readiness checks if all tasks are replaced already" in {
      Given("An app without health checks but readiness checks, as well as 1 task of this version")
      val f = new Fixture
      val check = ReadinessCheck()
      val port = PortDefinition(0, name = Some(check.portName))
      val app = AppDefinition(id = AbsolutePathId("/myApp"), role = "*", instances = 1, portDefinitions = Seq(port), readinessChecks = Seq(check))
      val i = f.createInstances(app)
      val (_, readyCheck) = f.readinessResults(i(0), check.name, ready = true)
      f.readinessCheckExecutor.execute(any[ReadinessCheckExecutor.ReadinessCheckSpec]) returns readyCheck
      val promise = Promise[Unit]()

      When("The replace actor is started")
      val ref = f.replaceActor(app, promise)
      watch(ref)
      f.sendState(app, app, ref, i, i, 0, 1)

      Then("It needs to wait for the readiness checks to pass")
      promise.future.futureValue
      expectTerminated(ref)
    }

    "wait for the readiness checks and health checks if all tasks are replaced already" in {
      Given("An app without health checks but readiness checks, as well as 1 task of this version")
      val f = new Fixture
      val ready = ReadinessCheck()
      val port = PortDefinition(0, name = Some(ready.portName))
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 1,
        portDefinitions = Seq(port),
        readinessChecks = Seq(ready),
        healthChecks = Set(MarathonHttpHealthCheck())
      )
      val i = f.createInstances(app)
      val (_, readyCheck) = f.readinessResults(i(0), ready.name, ready = true)
      f.readinessCheckExecutor.execute(any[ReadinessCheckExecutor.ReadinessCheckSpec]) returns readyCheck
      val promise = Promise[Unit]()

      When("The replace actor is started")
      val ref = f.replaceActor(app, promise)
      watch(ref)
      f.sendState(app, app, ref, i, i, 0, 1)

      Then("It needs to wait for the readiness checks to pass")
      promise.future.futureValue
      expectTerminated(ref)
    }

    "wait until the tasks are killed" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 5,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        upgradeStrategy = UpgradeStrategy(0.0))
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 5) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      f.sendState(app, newApp, ref, oldInstances, newInstances, 0, 5)

      verify(f.queue, timeout(1000)).resetDelay(newApp)

      promise.future.futureValue

      for (instance <- oldInstances)
        verify(f.tracker, never).setGoal(instance.instanceId, Goal.Decommissioned, GoalChangeReason.Upgrading)
      expectTerminated(ref)
    }

    "wait for health and readiness checks for new tasks" ignore {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 1,
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)),
        healthChecks = Set(MarathonHttpHealthCheck()),
        readinessChecks = Seq(ReadinessCheck()),
        upgradeStrategy = UpgradeStrategy(1.0, 1.0)
      )
      val newApp = app.copy(versionInfo = VersionInfo.forNewConfig(Timestamp(1)))

      val oldInstances = f.createInstances(app)
      val newInstances = f.createInstances(newApp)

      val promise = Promise[Unit]()
      f.queue.add(newApp, 1) returns Future.successful(Done)

      val ref = f.replaceActor(newApp, promise)
      watch(ref)

      // only one task is queued directly
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 0)
      val queueOrder = org.mockito.Mockito.inOrder(f.queue)
      eventually {
        queueOrder.verify(f.queue).add(_: AppDefinition, 1)
      }

      val newInstanceId = Instance.Id.forRunSpec(newApp.id)
      val newTaskId = Task.Id(newInstances(0).instanceId)

      //unhealthy
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 0, 1)
      eventually {
        verify(f.tracker, never).setGoal(any, any, any)
      }

      //unready
      //ref ! ReadinessCheckResult(ReadinessCheck.DefaultName, newTaskId, ready = false, None)
      //eventually {
      //  verify(f.tracker, never).setGoal(any, any, any)
      //}

      //healthy
      f.sendState(app, newApp, ref, oldInstances, newInstances, 1, 1)
      eventually {
        verify(f.tracker, never).setGoal(any, any, any)
      }

      //ready
      //ref ! ReadinessCheckResult(ReadinessCheck.DefaultName, newTaskId, ready = true, None)
      //eventually {
      //  verify(f.tracker, once).setGoal(any, any, any)
      //}

      promise.future.futureValue
      expectTerminated(ref)
    }

    // regression DCOS-54927
    "only handle InstanceChanged events of its own RunSpec" in {
      val f = new Fixture
      val app = AppDefinition(
        id = AbsolutePathId("/myApp"),
        role = "*",
        instances = 1,
        healthChecks = Set(MarathonHttpHealthCheck()),
        versionInfo = VersionInfo.forNewConfig(Timestamp(0)))
      val promise = Promise[Unit]()

      val ref = f.replaceActor(app, promise)
      watch(ref)

      // Test that Instance changed events for a different RunSpec are not handled by the actor
      val otherApp = AppDefinition(id = AbsolutePathId("/some-other-app"), role = "*")
      val bad_instance = f.runningInstance(otherApp)
      val good_instance = f.runningInstance(app)
      f.tracker.specInstancesSync(app.id, readAfterWrite = true) returns Seq(good_instance)
      ref ! HealthStatusResponse(Map(bad_instance.instanceId -> Seq(Health(bad_instance.instanceId).update(Healthy(null, null, null)))))

      eventually {
        verify(f.tracker, never).setGoal(any, any, any)
        verify(f.tracker, never).setGoal(any, any, any)
      }

      ref ! HealthStatusResponse(Map(good_instance.instanceId -> Seq(Health(good_instance.instanceId).update(Healthy(null, null, null)))))
      promise.future.futureValue
      expectTerminated(ref)
    }
  }
  class Fixture {
    val deploymentsManager: TestActorRef[Actor] = TestActorRef[Actor](Props.empty)
    val deploymentStatus = DeploymentStatus(Raml.toRaml(DeploymentPlan.empty), Raml.toRaml(DeploymentStep(Seq.empty)))
    val queue: LaunchQueue = mock[LaunchQueue]
    val tracker: InstanceTracker = mock[InstanceTracker]
    val readinessCheckExecutor: ReadinessCheckExecutor = mock[ReadinessCheckExecutor]
    val hostName = "host.some"
    val hostPorts = Seq(123)
    val metrics: Metrics = DummyMetrics

    tracker.setGoal(any, any, any) answers { args =>
      def sendKilled(instance: Instance, goal: Goal): Unit = {
        val updatedInstance = instance.copy(state = instance.state.copy(condition = Condition.Killed, goal = goal))
        val events = InstanceChangedEventsGenerator.events(updatedInstance, None, Timestamp(0), Some(instance.state))
        events.foreach(system.eventStream.publish)
      }

      val instanceId = args(0).asInstanceOf[Instance.Id]
      val maybeInstance = tracker.get(instanceId).futureValue
      maybeInstance.map { instance =>
        val goal = args(1).asInstanceOf[Goal]
        sendKilled(instance, goal)
        Future.successful(Done)
      }.getOrElse {
        Future.failed(throw new IllegalArgumentException(s"instance $instanceId is not ready in instance tracker when querying"))
      }
    }

    def runningInstance(app: AppDefinition): Instance = {
      TestInstanceBuilder.newBuilderForRunSpec(app, now = app.version)
        .addTaskWithBuilder().taskRunning().withNetworkInfo(hostName = Some(hostName), hostPorts = hostPorts).build()
        .getInstance()
    }

    def readinessResults(instance: Instance, checkName: String, ready: Boolean): (Cancellable, Source[ReadinessCheckResult, Cancellable]) = {
      val cancellable = new CancellableOnce(() => ())
      val source = Source(instance.tasksMap.values.map(task => ReadinessCheckResult(checkName, task.taskId, ready, None)).toList).
        mapMaterializedValue { _ => cancellable }
      (cancellable, source)
    }

    def createInstances(app: AppDefinition): Seq[Instance] = {
      var instances: Seq[Instance] = Seq[Instance]()
      for (i <- 0 until app.instances) {
        val instance = runningInstance(app)
        instances = instances :+ instance
        tracker.get(instance.instanceId) returns Future.successful(Some(instance))
      }
      return instances
    }

    def sendState(app: AppDefinition, newApp: AppDefinition, ref: ActorRef, oldInstances: Seq[Instance], newInstances: Seq[Instance], oldRunning: Int, newRunning: Int, newUnhealthy: Int = 0) = {
      var map = Map[Instance.Id, Seq[Health]]()
      for (i <- 0 until oldRunning)
        map += (oldInstances(i).instanceId -> Seq(Health(oldInstances(i).instanceId).update(Healthy(null, null, null))))
      for (i <- 0 until newRunning)
        map += (newInstances(i).instanceId -> Seq(Health(newInstances(i).instanceId).update(Healthy(null, null, null))))
      for (i <- 0 until newUnhealthy)
        map += (newInstances(i + newRunning).instanceId -> Seq(Health(newInstances(i + newRunning).instanceId).update(Unhealthy(null, null, null, ""))))
      tracker.specInstancesSync(app.id, readAfterWrite = true) returns oldInstances.take(oldRunning) ++ newInstances.take(newRunning + newUnhealthy)
      ref ! HealthStatusResponse(map)
    }

    def replaceActor(app: AppDefinition, promise: Promise[Unit]): ActorRef = system.actorOf(
      TaskReplaceActor.props(deploymentsManager, deploymentStatus, queue,
        tracker, system.eventStream, readinessCheckExecutor, app, promise, metrics)
    )
  }
}
