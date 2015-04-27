package mesosphere.mesos

import java.lang.{ Double => JDouble, Integer => JInt }

import scala.collection.JavaConverters._
import scala.collection.mutable

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import org.apache.log4j.Logger
import org.apache.mesos.Protos.Environment._
import org.apache.mesos.Protos._

import mesosphere.marathon._
import mesosphere.marathon.state.{ AppDefinition, PathId }
import mesosphere.marathon.tasks.{ ResourceMatcher, TaskTracker }
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.Protos.Constraint
import mesosphere.mesos.protos.{ Range, RangesResource, Resource, ScalarResource, SetResource }

import scala.collection.immutable.Seq
import scala.util.{ Failure, Success, Try }

class TaskBuilder(app: AppDefinition,
                  newTaskId: PathId => TaskID,
                  taskTracker: TaskTracker,
                  config: MarathonConf,
                  mapper: ObjectMapper = new ObjectMapper()) {

  import mesosphere.mesos.protos.Implicits._

  val log = Logger.getLogger(getClass.getName)

  def portsToResource(ports: Seq[JInt]): RangesResource = {
    RangesResource(Resource.PORTS, Seq(Range(0, 1)))
  }

  def buildIfMatches(offer: Offer): Option[(TaskInfo, Seq[Long])] = {
    val needed = offerMatches(offer) match {
      case Some(x) => x
      case _       => return None
    }

    val executor: Executor = if (app.executor == "") {
      Main.conf.executor
    }
    else {
      Executor.dispatch(app.executor)
    }

    val host: Option[String] = Some(offer.getHostname)

    val ports = needed.find(r => ResourceMatcher.getName(r) == Resource.PORTS)
      .getOrElse(RangesResource(Resource.PORTS, Seq())) match {
        case RangesResource(_, ranges, _) => ranges.flatMap(_.asScala()).to[Seq]
        case _                            => Seq()
      }

    val taskId = newTaskId(app.id)
    val builder = TaskInfo.newBuilder
      // Use a valid hostname to make service discovery easier
      .setName(app.id.toHostname)
      .setTaskId(taskId)
      .setSlaveId(offer.getSlaveId)

    for (res <- needed) {
      builder.addResources(res)
    }

    val containerProto: Option[ContainerInfo] =
      app.container.map { c =>
        val portMappings = c.docker.map { d =>
          d.portMappings.map { pms =>
            pms zip ports map {
              case (mapping, port) => mapping.copy(hostPort = port.toInt)
            }
          }
        }
        val containerWithPortMappings = portMappings match {
          case None => c
          case Some(newMappings) => c.copy(
            docker = c.docker.map { _.copy(portMappings = newMappings) }
          )
        }
        containerWithPortMappings.toMesos
      }

    executor match {
      case CommandExecutor() =>
        builder.setCommand(TaskBuilder.commandInfo(app, Some(taskId), host, ports))
        for (c <- containerProto) builder.setContainer(c)

      case PathExecutor(path) =>
        val executorId = f"marathon-${taskId.getValue}" // Fresh executor
        val executorPath = s"'$path'" // TODO: Really escape this.
        val cmd = app.cmd orElse app.args.map(_ mkString " ") getOrElse ""
        val shell = s"chmod ug+rx $executorPath && exec $executorPath $cmd"
        val command =
          TaskBuilder.commandInfo(app, Some(taskId), host, ports).toBuilder.setValue(shell)

        val info = ExecutorInfo.newBuilder()
          .setExecutorId(ExecutorID.newBuilder().setValue(executorId))
          .setCommand(command)
        for (c <- containerProto) info.setContainer(c)
        builder.setExecutor(info)
        val binary = new ByteArrayOutputStream()
        mapper.writeValue(binary, app)
        builder.setData(ByteString.copyFrom(binary.toByteArray))
    }

    // Mesos supports at most one health check, and only COMMAND checks
    // are currently implemented in the Mesos health check helper program.
    val mesosHealthChecks: Set[org.apache.mesos.Protos.HealthCheck] =
      app.healthChecks.flatMap { healthCheck =>
        if (healthCheck.protocol != Protocol.COMMAND) None
        else {
          try { Some(healthCheck.toMesos(ports.map(_.toInt))) }
          catch {
            case cause: Throwable =>
              log.warn(s"An error occurred with health check [$healthCheck]\n" +
                s"Error: [${cause.getMessage}]")
              None
          }
        }
      }

    if (mesosHealthChecks.size > 1) {
      val numUnusedChecks = mesosHealthChecks.size - 1
      log.warn(
        "Mesos supports one command health check per task.\n" +
          s"Task [$taskId] will run without " +
          s"$numUnusedChecks of its defined health checks."
      )
    }

    mesosHealthChecks.headOption.foreach(builder.setHealthCheck)

    Some(builder.build -> ports)
  }

  def portToResource(a: Seq[JInt]): Option[RangesResource] = {
    import mesosphere.mesos.protos._
    @annotation.tailrec
    def splitRange(lst: Seq[JInt], rangeBegin: Int, lastElem: Int,
                   resolved: Seq[Range]): Seq[Range] = {
      lst match {
        case hd :: tl =>
          if (hd == lastElem + 1)
            splitRange(tl, rangeBegin, hd, resolved)
          else
            splitRange(tl, hd, hd,
              resolved ++ Seq(Range(rangeBegin.toLong, lastElem.toLong)))
        case Nil =>
          resolved ++ Seq(Range(rangeBegin.toLong, lastElem.toLong))
      }
    }

    val req = a.toSet.toSeq.sorted
    if (req.isEmpty) None
    else Some(RangesResource(Resource.PORTS, req match {
      case hd :: tl => splitRange(tl, hd, hd, Seq())
    }))
  }

  private def offerMatches(offer: Offer): Option[Seq[Resource]] = {
    val matcher = new ResourceMatcher(app, offer)
    val result = matcher.resources(app.resources ++
      (portToResource(app.ports) match {
        case Some(x) => Seq(x)
        case _       => Nil
      }))

    val badConstraints: Set[Constraint] = {
      val runningTasks = taskTracker.get(app.id)
      app.constraints.filterNot { constraint =>
        Constraints.meetsConstraint(runningTasks, offer, constraint)
      }
    }

    if (badConstraints.nonEmpty) {
      log.warn(
        s"Offer did not satisfy constraints for app [${app.id}].\n" +
          s"Conflicting constraints are: [${badConstraints.mkString(", ")}]"
      )
      return None
    }
    log.debug("Met all constraints.")
    result
  }
}

object TaskBuilder {

  def commandInfo(app: AppDefinition, taskId: Option[TaskID], host: Option[String], ports: Seq[Long]): CommandInfo = {
    val containerPorts = for (pms <- app.portMappings) yield pms.map(_.containerPort)
    val declaredPorts = containerPorts.getOrElse(app.ports)
    val envMap: Map[String, String] =
      taskContextEnv(app, taskId) ++
        portsEnv(declaredPorts, ports) ++ host.map("HOST" -> _) ++
        app.env

    val builder = CommandInfo.newBuilder()
      .setEnvironment(environment(envMap))

    app.cmd match {
      case Some(cmd) if cmd.nonEmpty =>
        builder.setValue(cmd)
      case _ =>
        builder.setShell(false)
    }

    // args take precedence over command, if supplied
    app.args.foreach { argv =>
      builder.setShell(false)
      builder.addAllArguments(argv.asJava)
    }

    if (app.uris != null) {
      val uriProtos = app.uris.map(uri => {
        CommandInfo.URI.newBuilder()
          .setValue(uri)
          .setExtract(isExtract(uri))
          .build()
      })
      builder.addAllUris(uriProtos.asJava)
    }

    app.user.foreach(builder.setUser)

    builder.build
  }

  private def isExtract(stringuri: String): Boolean = {
    stringuri.endsWith(".tgz") ||
      stringuri.endsWith(".tar.gz") ||
      stringuri.endsWith(".tbz2") ||
      stringuri.endsWith(".tar.bz2") ||
      stringuri.endsWith(".txz") ||
      stringuri.endsWith(".tar.xz") ||
      stringuri.endsWith(".zip")
  }

  def environment(vars: Map[String, String]): Environment = {
    val builder = Environment.newBuilder()

    for ((key, value) <- vars) {
      val variable = Variable.newBuilder().setName(key).setValue(value)
      builder.addVariables(variable)
    }

    builder.build()
  }

  def portsEnv(definedPorts: Seq[Integer], assignedPorts: Seq[Long]): Map[String, String] = {
    if (assignedPorts.isEmpty) {
      return Map.empty
    }

    val env = mutable.HashMap.empty[String, String]

    assignedPorts.zipWithIndex.foreach {
      case (p, n) =>
        env += (s"PORT$n" -> p.toString)
    }

    definedPorts.zip(assignedPorts).foreach {
      case (defined, assigned) =>
        if (defined != AppDefinition.RandomPortValue) {
          env += (s"PORT_$defined" -> assigned.toString)
        }
    }

    env += ("PORT" -> assignedPorts.head.toString)
    env += ("PORTS" -> assignedPorts.mkString(","))
    env.toMap
  }

  def taskContextEnv(app: AppDefinition, taskId: Option[TaskID]): Map[String, String] =
    if (taskId.isEmpty) Map[String, String]()
    else Map(
      "MESOS_TASK_ID" -> taskId.get.getValue,
      "MARATHON_APP_ID" -> app.id.toString,
      "MARATHON_APP_VERSION" -> app.version.toString
    )

}
