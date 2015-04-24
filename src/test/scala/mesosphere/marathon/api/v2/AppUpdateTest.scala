package mesosphere.marathon.api.v2

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.immutable.Seq
import scala.concurrent.duration.DurationInt

import org.apache.mesos.{ Protos => mesos }

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import mesosphere.mesos.protos._

import javax.validation.Validation
import mesosphere.jackson.CaseClassModule
import mesosphere.marathon.MarathonSpec
import mesosphere.marathon.api.v2.json.MarathonModule
import mesosphere.marathon.api.validation.ValidHealthCheck
import mesosphere.marathon.health.HealthCheck
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.state.Container
import mesosphere.marathon.state.Container.Docker
import mesosphere.marathon.state.PathId
import mesosphere.marathon.state.PathId.StringPathId
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.state.UpgradeStrategy
import mesosphere.mesos.protos.Resource

class AppUpdateTest extends MarathonSpec {

  test("Validation") {
    val validator = Validation.buildDefaultValidatorFactory().getValidator

    def shouldViolate(update: AppUpdate, path: String, template: String) = {
      val violations = validator.validate(update).asScala
      assert(violations.exists(v =>
        v.getPropertyPath.toString == path && v.getMessageTemplate == template))
    }

    def shouldNotViolate(update: AppUpdate, path: String, template: String) = {
      val violations = validator.validate(update).asScala
      assert(!violations.exists(v =>
        v.getPropertyPath.toString == path && v.getMessageTemplate == template))
    }

    val update = AppUpdate()

    shouldViolate(
      update.copy(ports = Some(Seq(9000, 8080, 9000))),
      "ports",
      "Elements must be unique"
    )

  }

  test("SerializationRoundtrip") {
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    import mesosphere.jackson.CaseClassModule
    import mesosphere.marathon.api.v2.json.MarathonModule

    import mesosphere.marathon.state.AppDefinition

    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.registerModule(new MarathonModule)
    mapper.registerModule(CaseClassModule)

    val update0 = AppUpdate(container = Some(Container.Empty))
    val json0 = mapper.writeValueAsString(update0)
    val readResult0 = mapper.readValue(json0, classOf[AppUpdate])
    assert(readResult0 == update0)

    val update1 = AppUpdate(
      cmd = Some("sleep 60"),
      args = None,
      user = Some("nobody"),
      env = Some(Map("LANG" -> "en-US")),
      instances = Some(16),
      resources = Some(Seq(ScalarResource(Resource.CPUS, 2.0),
        ScalarResource(Resource.MEM, 256.0),
        ScalarResource(Resource.DISK, 1024.0))),
      // cpus = Some(2.0),
      // mem = Some(256.0),
      // disk = Some(1024.0),
      executor = Some("http://dl.corp.org/executors/some.executor"),
      constraints = Some(Set()),
      uris = Some(Seq("http://dl.corp.org/prodX-1.2.3.tgz")),
      ports = Some(Seq(0, 0)),
      backoff = Some(2.seconds),
      backoffFactor = Some(1.2),
      maxLaunchDelay = Some(1.minutes),
      container = Some(
        Container(
          `type` = mesos.ContainerInfo.Type.DOCKER,
          volumes = Nil,
          docker = Some(Docker(image = "docker:///group/image"))
        )
      ),
      healthChecks = Some(Set[HealthCheck]()),
      dependencies = Some(Set[PathId]()),
      upgradeStrategy = Some(UpgradeStrategy.empty),
      labels = Some(
        Map(
          "one" -> "aaa",
          "two" -> "bbb",
          "three" -> "ccc"
        )
      )
    )
    val json1 = mapper.writeValueAsString(update1)
    val readResult1 = mapper.readValue(json1, classOf[AppUpdate])
    assert(readResult1 == update1)

    val update2 = AppUpdate(container = Some(Container.Empty))
    val json2 = """
    {
      "cmd": null,
      "user": null,
      "env": null,
      "instances": null,
      "resources": null,
      "executor": null,
      "constraints": null,
      "uris": null,
      "ports": null,
      "backoffSeconds": null,
      "backoffFactor": null,
      "container": null,
      "healthChecks": null,
      "dependencies": null,
      "version": null
    }
  """
    val readResult2 = mapper.readValue(json2, classOf[AppUpdate])
    assert(readResult2 == update2)

    val update3 = AppUpdate()
    val json3 = "{}"
    val readResult3 = mapper.readValue(json3, classOf[AppUpdate])
    assert(readResult3 == update3)

    val update4 = AppUpdate(args = Some(Seq("a", "b", "c")))
    val json4 = """{ "args": ["a", "b", "c"] }"""
    val readResult4 = mapper.readValue(json4, classOf[AppUpdate])
    assert(readResult4 == update4)

  }

  test("'version' field can only be combined with 'id'") {
    assert(AppUpdate(version = Some(Timestamp.now())).onlyVersionOrIdSet)

    assert(AppUpdate(id = Some("foo".toPath), version = Some(Timestamp.now())).onlyVersionOrIdSet)

    intercept[Exception] {
      AppUpdate(cmd = Some("foo"), version = Some(Timestamp.now()))
    }
  }
}
