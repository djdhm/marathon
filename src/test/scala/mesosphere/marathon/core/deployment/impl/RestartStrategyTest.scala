package mesosphere.marathon
package core.deployment.impl

import mesosphere.UnitTest
import mesosphere.marathon.state._

class RestartStrategyTest extends UnitTest {

  val container = Some(Container.MesosDocker(volumes = Seq(VolumeWithMount(
    volume = PersistentVolume(name = None, PersistentVolumeInfo(123)),
    mount = VolumeMount(volumeName = None, mountPath = "path")
  ))
  ))

  import mesosphere.marathon.core.deployment.impl.TaskReplaceActor._
  "RestartStrategy" should {
    "strategy for resident app with 1 instance" in {
      Given("A resident app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 1,
        upgradeStrategy = UpgradeStrategy.forResidentTasks, // UpgradeStrategy(0.5, 0)
        container = container)

      When("the ignition strategy is computed")

      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 1, 0))

      Then("the app instance count is not exceeded")
      strategy.maxCapacity shouldBe 1

      And("we kill one task")
      strategy.nrToKillImmediately shouldBe 1
    }

    "strategy for starting resident app with 1 instance and no tasks running" in {
      Given("A resident app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 1,
        upgradeStrategy = UpgradeStrategy.forResidentTasks, // UpgradeStrategy(0.5, 0)
        container = container)

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 0, 1))

      Then("the app instance count is not exceeded")
      strategy.maxCapacity shouldBe 1

      And("we kill no task")
      strategy.nrToKillImmediately shouldBe 0
    }

    "strategy for resident app with 5 instances" in {
      Given("A resident app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 5,
        upgradeStrategy = UpgradeStrategy.forResidentTasks, // UpgradeStrategy(0.5, 0)
        container = container)

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 5, 0))

      Then("the app instance count is not exceeded")
      strategy.maxCapacity shouldBe 5

      And("we kill two tasks")
      strategy.nrToKillImmediately shouldBe 2
    }

    "strategy for resident app with 5 instances, already over capacity, maxHealthCapacity = 1" in {
      Given("A resident app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 5,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1, maximumOverCapacity = 0), // UpgradeStrategy(0.5, 0)
        container = container)

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 7, 0))

      Then("the app instance count is not exceeded")
      strategy.maxCapacity shouldBe 5

      And("we kill three tasks")
      strategy.nrToKillImmediately shouldBe 3
    }

    "strategy for resident app with 5 instances, already under capacity, maxHealthCapacity = 1" in {
      Given("A resident app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 5,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1, maximumOverCapacity = 0), // UpgradeStrategy(0.5, 0)
        container = container)

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 2, 3))

      Then("the app instance count is not exceeded")
      strategy.maxCapacity shouldBe 5

      And("we kill no tasks")
      strategy.nrToKillImmediately shouldBe 0
    }

    "strategy for resident app with 5 instances, already at capacity, maxHealthCapacity = 1" in {
      Given("A resident app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 5,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1, maximumOverCapacity = 0), // UpgradeStrategy(0.5, 0)
        container = container)

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 5, 0))

      Then("the app instance count is not exceeded")
      strategy.maxCapacity shouldBe 5

      And("we kill one task")
      strategy.nrToKillImmediately shouldBe 1
    }

    "strategy for normal app with 1 instance" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 1,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 0.5, maximumOverCapacity = 0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 1, 0))

      Then("the app instance count is exceeded by one")
      strategy.maxCapacity shouldBe 2

      And("we kill 0 tasks immediately")
      strategy.nrToKillImmediately shouldBe 0
    }

    "strategy for normal app with 1 instance and no tasks running" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 1,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 0.5, maximumOverCapacity = 0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 0, 0))

      Then("the app instance count does not need to be exceeded (since we can start a task without kills)")
      strategy.maxCapacity shouldBe 1

      And("we kill no tasks immediately")
      strategy.nrToKillImmediately shouldBe 0
    }

    "strategy for normal app with 5 instances, already deployed" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 5,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 0.5, maximumOverCapacity = 0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(4, 1, 0, 0))

      Then("we kill no tasks immediately")
      strategy.nrToKillImmediately shouldBe 0
    }

    "maxCapacity strategy for normal app is not exceeded when a task can be killed immediately" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 2,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 0.5, maximumOverCapacity = 0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 2, 0))

      Then("the app instance count is exceeded by one")
      strategy.maxCapacity shouldBe 2

      And("we kill one task immediately")
      strategy.nrToKillImmediately shouldBe 1
    }

    "less running tasks than actual instance count and less than min health capacity" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 10,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 0.1, maximumOverCapacity = 0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 1, 0))

      Then("the maxCapacity equals the app.instance count")
      strategy.maxCapacity shouldBe 10

      And("no tasks are killed immediately")
      strategy.nrToKillImmediately shouldBe 0
    }

    "max over capacity 1" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 10,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 0.1, maximumOverCapacity = 1.0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 10, 0))

      Then("the maxCapacity is double the app.instance count")
      strategy.maxCapacity shouldBe 20

      And("9 tasks are killed immediately")
      strategy.nrToKillImmediately shouldBe 9
    }

    "maxOverCapacity 1 and minHealth 1" in {
      Given("A normal app")
      val app = AppDefinition(
        id = AbsolutePathId("/app"),
        role = "*",
        instances = 10,
        upgradeStrategy = UpgradeStrategy(minimumHealthCapacity = 1, maximumOverCapacity = 1.0))

      When("the ignition strategy is computed")
      val strategy = computeRestartStrategy(app, new TransitionState(0, 0, 10, 0))

      Then("the maxCapacity is double the app.instance count")
      strategy.maxCapacity shouldBe 20

      And("no tasks are killed immediately")
      strategy.nrToKillImmediately shouldBe 0
    }
  }
}
