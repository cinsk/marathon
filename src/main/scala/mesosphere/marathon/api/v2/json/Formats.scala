package mesosphere.marathon.api.v2.json

import java.lang.{ Double => JDouble }

import mesosphere.marathon.Protos.Constraint.Operator
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.Protos.{ Constraint, MarathonTask }
import mesosphere.marathon.api.v2.AppUpdate
import mesosphere.marathon.event._
import mesosphere.marathon.health.{ Health, HealthCheck }
import mesosphere.marathon.state.Container.Docker.PortMapping
import mesosphere.marathon.state.Container.{ Docker, Volume }
import mesosphere.marathon.state._
import mesosphere.marathon.upgrade._
import mesosphere.mesos.protos._
import org.apache.mesos.Protos.ContainerInfo.DockerInfo.Network
import org.apache.mesos.{ Protos => mesos }
import org.apache.mesos.Protos.{ Resource => ProtoResource }
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json._
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.control.NonFatal

object Formats extends Formats {

  implicit class ReadsWithDefault[A](val reads: Reads[Option[A]]) extends AnyVal {
    def withDefault(a: A): Reads[A] = reads.map(_.getOrElse(a))
  }

  implicit class FormatWithDefault[A](val m: OFormat[Option[A]]) extends AnyVal {
    def withDefault(a: A): OFormat[A] = m.inmap(_.getOrElse(a), Some(_))
  }

  implicit class ReadsAsSeconds(val reads: Reads[Long]) extends AnyVal {
    def asSeconds: Reads[FiniteDuration] = reads.map(_.seconds)
  }

  implicit class FormatAsSeconds(val format: OFormat[Long]) extends AnyVal {
    def asSeconds: OFormat[FiniteDuration] =
      format.inmap(
        _.seconds,
        _.toSeconds
      )
  }
}

trait Formats
    extends AppDefinitionFormats
    with HealthCheckFormats
    with ContainerFormats
    with DeploymentFormats
    with EventFormats {
  import scala.collection.JavaConverters._

  implicit lazy val ValueWrites: Writes[mesos.Value] = Writes { value =>
    import scala.collection.JavaConverters._

    value.getType() match {
      case mesos.Value.Type.SCALAR => JsNumber(value.getScalar.getValue)
      case mesos.Value.Type.SET =>
        Json.toJson(value.getSet().getItemList().asScala.toList)
      case mesos.Value.Type.TEXT => JsString(value.getText.getValue)
      case mesos.Value.Type.RANGES =>
        Json.toJson(for (elem <- value.getRanges().getRangeList().asScala)
          yield Seq(elem.getBegin(), elem.getEnd()))

    }
  }

  implicit def valueFormat: Format[mesos.Value] = {
    val reads = Reads[mesos.Value] {
      case JsNumber(num) =>
        JsSuccess(mesos.Value.newBuilder().setScalar(mesos.Value.Scalar.newBuilder().setValue(num.toDouble)).build())
      case JsString(text) =>
        JsSuccess(mesos.Value.newBuilder().setText(
          mesos.Value.Text.newBuilder().setValue(text)).build())
      case _ => JsError("not supported yet")
    }
    val writes = Writes[mesos.Value] { value =>
      ValueWrites.writes(value)
    }
    Format(reads, writes)
  }

  implicit lazy val TaskFailureWrites: Writes[TaskFailure] = Writes { failure =>
    Json.obj(
      "appId" -> failure.appId,
      "host" -> failure.host,
      "message" -> failure.message,
      "state" -> failure.state.name(),
      "taskId" -> failure.taskId.getValue,
      "timestamp" -> failure.timestamp,
      "version" -> failure.version
    )
  }

  implicit lazy val MarathonTaskWrites: Writes[MarathonTask] = Writes { task =>
    Json.obj(
      "id" -> task.getId,
      "host" -> (if (task.hasHost) task.getHost else JsNull),
      "ports" -> task.getPortsList.asScala,
      "startedAt" -> (if (task.getStartedAt != 0) Timestamp(task.getStartedAt) else JsNull),
      "stagedAt" -> (if (task.getStagedAt != 0) Timestamp(task.getStagedAt) else JsNull),
      "version" -> task.getVersion
    )
  }

  implicit lazy val EnrichedTaskWrites: Writes[EnrichedTask] = Writes { task =>
    val taskJson = MarathonTaskWrites.writes(task.task).as[JsObject]

    val enrichedJson = taskJson ++ Json.obj(
      "appId" -> task.appId
    )

    val withServicePorts = if (task.servicePorts.nonEmpty)
      enrichedJson ++ Json.obj("servicePorts" -> task.servicePorts)
    else
      enrichedJson

    if (task.healthCheckResults.nonEmpty)
      withServicePorts ++ Json.obj("healthCheckResults" -> task.healthCheckResults)
    else
      withServicePorts
  }

  implicit lazy val PathIdFormat: Format[PathId] = Format(
    Reads.of[String](Reads.minLength[String](1)).map(PathId(_)),
    Writes[PathId] { id => JsString(id.toString) }
  )

  implicit lazy val TimestampFormat: Format[Timestamp] = Format(
    Reads.of[String].map(Timestamp(_)),
    Writes[Timestamp] { t => JsString(t.toString) }
  )

  implicit lazy val IntegerFormat: Format[Integer] = Format(
    Reads.of[Int].map(Int.box),
    Writes[Integer] { i => JsNumber(i.intValue) }
  )
  implicit lazy val DoubleFormat: Format[JDouble] = Format(
    Reads.of[Double].map(Double.box),
    Writes[JDouble] { d => JsNumber(d.doubleValue()) }
  )

  implicit lazy val CommandFormat: Format[Command] = Json.format[Command]

  implicit lazy val ParameterFormat: Format[Parameter] = (
    (__ \ "key").format[String] ~
    (__ \ "value").format[String]
  )(Parameter(_, _), unlift(Parameter.unapply))

  /*
 * Helpers
 */

  def uniquePorts: Reads[Seq[Integer]] = Format.of[Seq[Integer]].filter { ports =>
    val withoutRandom = ports.filterNot(_ == AppDefinition.RandomPortValue)
    withoutRandom.distinct.size == withoutRandom.size
  }

  def minValue[A](min: A)(implicit O: Ordering[A], reads: Reads[A]): Reads[A] =
    Reads.filterNot[A](ValidationError(s"value must not be less than $min"))(x => O.lt(x, min))(reads)

  def greaterThan[A](x: A)(implicit Ord: Ordering[A], reads: Reads[A]): Reads[A] =
    Reads.filter[A](ValidationError(s"value must be greater than $x"))(y => Ord.gt(y, x))(reads)

  def enumFormat[A <: java.lang.Enum[A]](read: String => A, errorMsg: String => String): Format[A] = {
    val reads = Reads[A] {
      case JsString(str) =>
        try {
          JsSuccess(read(str))
        }
        catch {
          case _: IllegalArgumentException => JsError(errorMsg(str))
        }

      case x => JsError(s"expected string, got $x")
    }

    val writes = Writes[A] { a: A => JsString(a.name) }

    Format(reads, writes)
  }
}

trait ContainerFormats {
  import Formats._

  implicit lazy val NetworkFormat: Format[Network] =
    enumFormat(Network.valueOf, str => s"$str is not a valid network type")

  implicit lazy val PortMappingFormat: Format[Docker.PortMapping] = (
    (__ \ "containerPort").format[Integer] ~
    (__ \ "hostPort").formatNullable[Integer].withDefault(0) ~
    (__ \ "servicePort").formatNullable[Integer].withDefault(0) ~
    (__ \ "protocol").formatNullable[String].withDefault("tcp")
  )(PortMapping(_, _, _, _), unlift(PortMapping.unapply))

  implicit lazy val DockerFormat: Format[Docker] = (
    (__ \ "image").format[String] ~
    (__ \ "network").formatNullable[Network] ~
    (__ \ "portMappings").formatNullable[Seq[Docker.PortMapping]] ~
    (__ \ "privileged").formatNullable[Boolean].withDefault(false) ~
    (__ \ "parameters").formatNullable[Seq[Parameter]].withDefault(Seq.empty)
  )(Docker(_, _, _, _, _), unlift(Docker.unapply))

  implicit val ModeFormat: Format[mesos.Volume.Mode] =
    enumFormat(mesos.Volume.Mode.valueOf, str => s"$str is not a valid mode")

  implicit lazy val VolumeFormat: Format[Volume] = (
    (__ \ "containerPath").format[String] ~
    (__ \ "hostPath").format[String] ~
    (__ \ "mode").format[mesos.Volume.Mode]
  )(Volume(_, _, _), unlift(Volume.unapply))

  implicit val ContainerTypeFormat: Format[mesos.ContainerInfo.Type] =
    enumFormat(mesos.ContainerInfo.Type.valueOf, str => s"$str is not a valid container type")

  implicit lazy val ContainerFormat: Format[Container] = (
    (__ \ "type").formatNullable[mesos.ContainerInfo.Type].withDefault(mesos.ContainerInfo.Type.DOCKER) ~
    (__ \ "volumes").formatNullable[Seq[Volume]].withDefault(Nil) ~
    (__ \ "docker").formatNullable[Docker]
  )(Container(_, _, _), unlift(Container.unapply))
}

trait DeploymentFormats {
  import Formats._

  implicit lazy val ByteArrayFormat: Format[Array[Byte]] =
    Format(
      Reads.of[Seq[Int]].map(_.map(_.toByte).toArray),
      Writes { xs =>
        JsArray(xs.to[Seq].map(b => JsNumber(b.toInt)))
      }
    )
  implicit lazy val GroupFormat: Format[Group] = (
    (__ \ "id").format[PathId] ~
    (__ \ "apps").formatNullable[Set[AppDefinition]].withDefault(Group.DefaultApps) ~
    (__ \ "groups").lazyFormatNullable(implicitly[Format[Set[Group]]]).withDefault(Group.DefaultGroups) ~
    (__ \ "dependencies").formatNullable[Set[PathId]].withDefault(Group.DefaultDependencies) ~
    (__ \ "version").formatNullable[Timestamp].withDefault(Group.DefaultVersion)
  )(Group(_, _, _, _, _), unlift(Group.unapply))

  implicit lazy val URLToStringMapFormat: Format[Map[java.net.URL, String]] = Format(
    Reads.of[Map[String, String]]
      .map(
        _.map { case (k, v) => new java.net.URL(k) -> v }
      ),
    Writes[Map[java.net.URL, String]] { m =>
      val mapped = m.map { case (k, v) => k.toString -> v }
      Json.toJson(m)
    }
  )
  implicit lazy val DeploymentActionWrites: Writes[DeploymentAction] = Writes { action =>
    Json.obj(
      "type" -> action.getClass.getSimpleName,
      "app" -> action.app.id
    )
  }

  implicit lazy val DeploymentStepWrites: Writes[DeploymentStep] = Json.writes[DeploymentStep]
  implicit lazy val DeploymentPlanWrites: Writes[DeploymentPlan] = (
    (__ \ "id").write[String] ~
    (__ \ "original").write[Group] ~
    (__ \ "target").write[Group] ~
    (__ \ "steps").write[Seq[DeploymentStep]] ~
    (__ \ "version").write[Timestamp]
  )(unlift(DeploymentPlan.unapply))
}

trait EventFormats {
  import Formats._

  implicit lazy val AppTerminatedEventWrites: Writes[AppTerminatedEvent] = Json.writes[AppTerminatedEvent]
  implicit lazy val ApiPostEventWrites: Writes[ApiPostEvent] = Json.writes[ApiPostEvent]
  implicit lazy val SubscribeWrites: Writes[Subscribe] = Json.writes[Subscribe]
  implicit lazy val UnsubscribeWrites: Writes[Unsubscribe] = Json.writes[Unsubscribe]
  implicit lazy val AddHealthCheckWrites: Writes[AddHealthCheck] = Json.writes[AddHealthCheck]
  implicit lazy val RemoveHealthCheckWrites: Writes[RemoveHealthCheck] = Json.writes[RemoveHealthCheck]
  implicit lazy val FailedHealthCheckWrites: Writes[FailedHealthCheck] = Json.writes[FailedHealthCheck]
  implicit lazy val HealthStatusChangedWrites: Writes[HealthStatusChanged] = Json.writes[HealthStatusChanged]
  implicit lazy val GroupChangeSuccessWrites: Writes[GroupChangeSuccess] = Json.writes[GroupChangeSuccess]
  implicit lazy val GroupChangeFailedWrites: Writes[GroupChangeFailed] = Json.writes[GroupChangeFailed]
  implicit lazy val DeploymentSuccessWrites: Writes[DeploymentSuccess] = Json.writes[DeploymentSuccess]
  implicit lazy val DeploymentFailedWrites: Writes[DeploymentFailed] = Json.writes[DeploymentFailed]
  implicit lazy val DeploymentStatusWrites: Writes[DeploymentStatus] = Json.writes[DeploymentStatus]
  implicit lazy val DeploymentStepSuccessWrites: Writes[DeploymentStepSuccess] = Json.writes[DeploymentStepSuccess]
  implicit lazy val DeploymentStepFailureWrites: Writes[DeploymentStepFailure] = Json.writes[DeploymentStepFailure]
  implicit lazy val MesosStatusUpdateEventWrites: Writes[MesosStatusUpdateEvent] = Json.writes[MesosStatusUpdateEvent]
  implicit lazy val MesosFrameworkMessageEventWrites: Writes[MesosFrameworkMessageEvent] =
    Json.writes[MesosFrameworkMessageEvent]
  implicit lazy val SchedulerDisconnectedEventWrites: Writes[SchedulerDisconnectedEvent] =
    Json.writes[SchedulerDisconnectedEvent]
  implicit lazy val SchedulerRegisteredEventWritesWrites: Writes[SchedulerRegisteredEvent] =
    Json.writes[SchedulerRegisteredEvent]
  implicit lazy val SchedulerReregisteredEventWritesWrites: Writes[SchedulerReregisteredEvent] =
    Json.writes[SchedulerReregisteredEvent]
}

trait HealthCheckFormats {
  import Formats._

  /*
   * HealthCheck related formats
   */

  implicit lazy val HealthWrites: Writes[Health] = Writes { health =>
    Json.obj(
      "alive" -> health.alive,
      "consecutiveFailures" -> health.consecutiveFailures,
      "firstSuccess" -> health.firstSuccess,
      "lastFailure" -> health.lastFailure,
      "lastSuccess" -> health.lastSuccess,
      "taskId" -> health.taskId
    )
  }

  implicit lazy val ProtocolFormat: Format[Protocol] =
    enumFormat(Protocol.valueOf, str => s"$str is not a valid protocol")

  implicit lazy val HealtCheckFormat: Format[HealthCheck] = {
    import mesosphere.marathon.health.HealthCheck._

    (
      (__ \ "path").formatNullable[Option[String]].withDefault(DefaultPath) ~
      (__ \ "protocol").formatNullable[Protocol].withDefault(DefaultProtocol) ~
      (__ \ "portIndex").formatNullable[Integer].withDefault(DefaultPortIndex) ~
      (__ \ "command").formatNullable[Command] ~
      (__ \ "gracePeriodSeconds").formatNullable[Long].withDefault(DefaultGracePeriod.toSeconds).asSeconds ~
      (__ \ "intervalSeconds").formatNullable[Long].withDefault(DefaultInterval.toSeconds).asSeconds ~
      (__ \ "timeoutSeconds").formatNullable[Long].withDefault(DefaultTimeout.toSeconds).asSeconds ~
      (__ \ "maxConsecutiveFailures").formatNullable[Integer].withDefault(DefaultMaxConsecutiveFailures) ~
      (__ \ "ignoreHttp1xx").formatNullable[Boolean].withDefault(DefaultIgnoreHttp1xx)
    )(HealthCheck.apply, unlift(HealthCheck.unapply))
  }
}

trait AppDefinitionFormats {
  import Formats._

  implicit lazy val IdentifiableWrites = Json.writes[Identifiable]

  implicit lazy val UpgradeStrategyWrites = Json.writes[UpgradeStrategy]
  implicit lazy val UpgradeStrategyReads: Reads[UpgradeStrategy] = {
    import mesosphere.marathon.state.AppDefinition._
    (
      (__ \ "minimumHealthCapacity").readNullable[JDouble].withDefault(DefaultUpgradeStrategy.minimumHealthCapacity) ~
      (__ \ "maximumOverCapacity").readNullable[JDouble].withDefault(DefaultUpgradeStrategy.maximumOverCapacity)
    )(UpgradeStrategy(_, _))
  }

  implicit lazy val ConstraintFormat: Format[Constraint] = Format(
    Reads.of[Seq[String]].map { seq =>
      val builder = Constraint
        .newBuilder()
        .setField(seq(0))
        .setOperator(Operator.valueOf(seq(1)))
      if (seq.size == 3) builder.setValue(seq(2))
      builder.build()
    },
    Writes[Constraint] { constraint =>
      val builder = Seq.newBuilder[JsString]
      builder += JsString(constraint.getField)
      builder += JsString(constraint.getOperator.name)
      if (constraint.hasValue) builder += JsString(constraint.getValue)
      JsArray(builder.result())
    }
  )

  implicit lazy val ResourceFormat: Format[Resource] = {
    val reads = new Reads[Resource] {
      def reads(json: JsValue): JsResult[Resource] = json match {
        case JsObject(pairs) =>
          val name = (json \ "name").as[String]
          val role = (json \ "role").asOpt[JsString].getOrElse(JsString("*")).as[String]
          val value = (json \ "value")

          value match {
            case JsNumber(num) if (num >= 0) => name match {
              case Resource.CPUS if num == 0 => JsError("cpus must be larger than zero.")
              case Resource.MEM if num == 0  => JsError("mem must be larger than zero.")
              case _                         => JsSuccess(ScalarResource(name, num.toDouble, role))
            }

            case JsArray(arr) if !arr.isEmpty =>
              try {
                val s: Seq[String] = value.as[Seq[String]]
                JsSuccess(SetResource(name, s.toSet, role))
              }
              catch {
                case e @ JsResultException(_) =>
                  try {
                    JsSuccess(RangesResource(name,
                      (for (o <- arr)
                        yield o match {
                        case obj @ JsObject(_) => Range((obj \ "begin").as[Long], (obj \ "end").as[Long])
                        case _                 => throw e
                      }).toSeq,
                      role))
                  }
                  catch {
                    case JsResultException(msg) => JsError(msg)
                  }
              }
            case _ => JsError("Not supported yet")
          }
        case _ => JsError("Not supported yet")
      }
    }
    val writes = Writes[Resource] {
      case ScalarResource(name, value, role) =>
        Json.toJson(Map("name" -> JsString(name),
          "value" -> JsNumber(value),
          "role" -> JsString(role)))
      case SetResource(name, items, role) =>
        Json.toJson(Map("name" -> JsString(name),
          "role" -> JsString(role),
          "value" -> Json.toJson(items.toList)))
      case RangesResource(name, ranges, role) =>
        // ranges: Seq[Range]
        Json.toJson(Map("name" -> JsString(name),
          "role" -> JsString(role),
          "value" -> Json.toJson(for (r <- ranges)
            yield JsArray(Seq(JsNumber(r.begin), JsNumber(r.end))))))
    }

    Format(reads, writes)
  }

  implicit lazy val ProtoResourceFormat: Format[ProtoResource] = {
    val reads = new Reads[ProtoResource] {
      def reads(json: JsValue): JsResult[ProtoResource] = json match {
        case JsObject(pairs) =>
          val name = (json \ "name").as[String]
          val role = (json \ "role").asOpt[JsString].getOrElse(JsString("*")).as[String]
          val value = (json \ "value")

          val builder = ProtoResource
            .newBuilder()
            .setName(name)
            .setRole(role)

          value match {
            case JsNumber(num) =>
              name match {
                case Resource.CPUS if (num <= 0.0) =>
                  JsError(s"${Resource.CPUS} should be larger than zero")
                case _ =>
                  JsSuccess(builder
                    .setType(mesos.Value.Type.SCALAR)
                    .setScalar(mesos.Value.Scalar
                      .newBuilder()
                      .setValue(num.toDouble)
                      .build())
                    .build())
              }
            case _ => JsError("Not supported yet")
          }
        case _ => JsError("Not supported yet")
      }
    }
    val writes = new Writes[ProtoResource] {
      def writes(r: ProtoResource): JsValue = {
        import mesosphere.mesos.protos.Implicits._
        val res: Resource = r

        res match {
          case ScalarResource(name, value, role) =>
            Json.toJson(Map("name" -> JsString(name),
              "value" -> JsNumber(value),
              "role" -> JsString(role)))
          case SetResource(name, items, role) =>
            Json.toJson(Map("name" -> JsString(name),
              "role" -> JsString(role),
              "value" -> Json.toJson(items.toList)))
          case RangesResource(name, ranges, role) =>
            // ranges: Seq[Range]
            Json.toJson(Map("name" -> JsString(name),
              "role" -> JsString(role),
              "value" -> Json.toJson(for (r <- ranges)
                yield JsArray(Seq(JsNumber(r.begin), JsNumber(r.end))))))
        }
      }
    }
    Format(reads, writes)
  }

  // val p = (__ \ "tmp").readNullable[ProtoResource]
  // val q = (__ \ "tmp").readNullable[Constraint]
  // val r: JsResult[ProtoResource] = ProtoResourceFormat.reads(Json.parse(""""""))
  // 
  // val respath = (__ \ "resources").readNullable[Seq[ProtoResource]].withDefault(AppDefinition.DefaultResources)

  implicit lazy val AppDefinitionReads: Reads[AppDefinition] = {
    import mesosphere.marathon.state.AppDefinition._

    val executorPattern = "^(//cmd)|(/?[^/]+(/[^/]+)*)|$".r

    // val r = (__ \ "tmp").readNullable[ProtoResource]
    // val s = (__ \ "tmp").readNullable[Constraint]

    (
      (__ \ "id").read[PathId].filterNot(_.isRoot) ~
      (__ \ "cmd").readNullable[String] ~
      (__ \ "args").readNullable[Seq[String]] ~
      (__ \ "user").readNullable[String] ~
      (__ \ "env").readNullable[Map[String, String]].withDefault(DefaultEnv) ~
      (__ \ "instances").readNullable[Integer](minValue(0)).withDefault(DefaultInstances) ~
      // (__ \ "resources").readNullable[Seq[ProtoResource]].withDefault(DefaultResources).flatMap {
      //   rseq =>
      //     rseq.find(r => r.getName() == "cpus") match {
      //       case Some(x) if (x.getScalar().getValue().toDouble >= 0.0) =>
      //         implicitly[Reads[Seq[ProtoResource]]]
      //       case _ => new Reads[Seq[ProtoResource]] {
      //         def reads(json: JsValue) = { JsError("") }
      //       }
      //     }
      // } ~
      (__ \ "resources").readNullable[Seq[Resource]].withDefault(DefaultResources) ~
      // (__ \ "cpus").readNullable[JDouble](greaterThan(0.0)).withDefault(DefaultCpus) ~
      // (__ \ "mem").readNullable[JDouble].withDefault(DefaultMem) ~
      // (__ \ "disk").readNullable[JDouble].withDefault(DefaultDisk) ~
      (__ \ "executor").readNullable[String](Reads.pattern(executorPattern)).withDefault(DefaultExecutor) ~
      (__ \ "constraints").readNullable[Set[Constraint]].withDefault(DefaultConstraints) ~
      (__ \ "uris").readNullable[Seq[String]].withDefault(DefaultUris) ~
      (__ \ "storeUrls").readNullable[Seq[String]].withDefault(DefaultStoreUrls) ~
      (__ \ "ports").readNullable[Seq[Integer]](uniquePorts).withDefault(DefaultPorts) ~
      (__ \ "requirePorts").readNullable[Boolean].withDefault(DefaultRequirePorts) ~
      (__ \ "backoffSeconds").readNullable[Long].withDefault(DefaultBackoff.toSeconds).asSeconds ~
      (__ \ "backoffFactor").readNullable[Double].withDefault(DefaultBackoffFactor) ~
      (__ \ "maxLaunchDelaySeconds").readNullable[Long].withDefault(DefaultMaxLaunchDelay.toSeconds).asSeconds ~
      (__ \ "container").readNullable[Container] ~
      (__ \ "healthChecks").readNullable[Set[HealthCheck]].withDefault(DefaultHealthChecks) ~
      (__ \ "dependencies").readNullable[Set[PathId]].withDefault(DefaultDependencies)
    )(AppDefinition(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)).flatMap { app =>
        // necessary because of case class limitations (good for another 21 fields)
        case class ExtraFields(
          upgradeStrategy: UpgradeStrategy,
          labels: Map[String, String],
          version: Timestamp)

        val extraReads: Reads[ExtraFields] =
          (
            (__ \ "upgradeStrategy").readNullable[UpgradeStrategy].withDefault(DefaultUpgradeStrategy) ~
            (__ \ "labels").readNullable[Map[String, String]].withDefault(DefaultLabels) ~
            (__ \ "version").readNullable[Timestamp].withDefault(Timestamp.now())
          )(ExtraFields(_, _, _))

        extraReads.map { extraFields =>
          app.copy(
            upgradeStrategy = extraFields.upgradeStrategy,
            labels = extraFields.labels,
            version = extraFields.version
          )
        }
      }
  }

  implicit lazy val AppDefinitionWrites: Writes[AppDefinition] = {
    implicit val durationWrites = Writes[FiniteDuration] { d =>
      JsNumber(d.toSeconds)
    }

    Writes[AppDefinition] { app =>
      try {
        Json.obj(
          "id" -> app.id.toString,
          "cmd" -> app.cmd,
          "args" -> app.args,
          "user" -> app.user,
          "env" -> app.env,
          "instances" -> app.instances,
          "resources" -> app.resources,
          //          "cpus" -> app.cpus,
          //          "mem" -> app.mem,
          //          "disk" -> app.disk,
          "executor" -> app.executor,
          "constraints" -> app.constraints,
          "uris" -> app.uris,
          "storeUrls" -> app.storeUrls,
          "ports" -> app.ports,
          "requirePorts" -> app.requirePorts,
          "backoffSeconds" -> app.backoff,
          "backoffFactor" -> app.backoffFactor,
          "maxLaunchDelaySeconds" -> app.maxLaunchDelay,
          "container" -> app.container,
          "healthChecks" -> app.healthChecks,
          "dependencies" -> app.dependencies,
          "upgradeStrategy" -> app.upgradeStrategy,
          "labels" -> app.labels,
          "version" -> app.version
        )
      }
      catch {
        case NonFatal(e) =>
          throw e
      }
    }
  }

  implicit lazy val AppUpdateReads: Reads[AppUpdate] = {

    (
      (__ \ "id").readNullable[PathId].filterNot(_.exists(_.isRoot)) ~
      (__ \ "cmd").readNullable[String] ~
      (__ \ "args").readNullable[Seq[String]] ~
      (__ \ "user").readNullable[String] ~
      (__ \ "env").readNullable[Map[String, String]] ~
      (__ \ "instances").readNullable[Integer](minValue(0)) ~
      (__ \ "resources").readNullable[Seq[Resource]] ~
      // (__ \ "cpus").readNullable[JDouble](greaterThan(0.0)) ~
      // (__ \ "mem").readNullable[JDouble] ~
      // (__ \ "disk").readNullable[JDouble] ~
      (__ \ "executor").readNullable[String](Reads.pattern("^(//cmd)|(/?[^/]+(/[^/]+)*)|$".r)) ~
      (__ \ "constraints").readNullable[Set[Constraint]] ~
      (__ \ "uris").readNullable[Seq[String]] ~
      (__ \ "storeUrls").readNullable[Seq[String]] ~
      (__ \ "ports").readNullable[Seq[Integer]](uniquePorts) ~
      (__ \ "requirePorts").readNullable[Boolean] ~
      (__ \ "backoffSeconds").readNullable[Long].map(_.map(_.seconds)) ~
      (__ \ "backoffFactor").readNullable[JDouble] ~
      (__ \ "maxLaunchDelaySeconds").readNullable[Long].map(_.map(_.seconds)) ~
      (__ \ "container").readNullable[Container] ~
      (__ \ "healthChecks").readNullable[Set[HealthCheck]] ~
      (__ \ "dependencies").readNullable[Set[PathId]]
    )(AppUpdate(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)).flatMap { update =>
        // necessary because of case class limitations (good for another 21 fields)
        case class ExtraFields(
          upgradeStrategy: Option[UpgradeStrategy],
          labels: Option[Map[String, String]],
          version: Option[Timestamp])

        val extraReads: Reads[ExtraFields] =
          (
            (__ \ "upgradeStrategy").readNullable[UpgradeStrategy] ~
            (__ \ "labels").readNullable[Map[String, String]] ~
            (__ \ "version").readNullable[Timestamp]
          )(ExtraFields(_, _, _))

        extraReads.map { extraFields =>
          update.copy(
            upgradeStrategy = extraFields.upgradeStrategy,
            labels = extraFields.labels,
            version = extraFields.version
          )
        }

      }
  }
}
