package mesosphere.mesos

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
import mesosphere.marathon.tasks.{ PortsMatcher, TaskTracker }
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

  def neededResources(offer: Offer): Option[Map[String, Resource]] = {
    // TODO: Make sure that RangesResource is not empty (esp. ports)

    val portsMatcher = new PortsMatcher(app, offer)

    val offeredSeq = offer.getResourcesList().asScala.map {
      r => implicitly[Resource](r)
    }.toSeq

    val result = (for ((name, neededRes) <- app.resourcesMap) yield {
      if (name == Resource.PORTS) {
        // TODO: for the Resource.PORTS, use PortsMatcher instead of transformNeeds
        portsMatcher.portRanges match {
          case Some(rangeRes) => (name, rangeRes)
          case None =>
            log.warn("App ports are not available in the offer.")
            return None
        }
      }
      else {
        val takenResOption = offeredSeq.find {
          offeredRes => transformNeeds(neededRes, offeredRes).isDefined
        }

        takenResOption match {
          case Some(takenRes) => (name, takenRes)
          case None           => return None
        }
      }
    }).toMap

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
    Some(result)
  }

  def buildIfMatches(offer: Offer): Option[(TaskInfo, Seq[Long])] = {
    val neededMap = neededResources(offer) match {
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

    val ports = neededMap.get(Resource.PORTS)
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

    for ((_, resNeeded) <- neededMap) {
      builder.addResources(resNeeded)
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

  def buildIfMatchesOld(offer: Offer): Option[(TaskInfo, Seq[Long])] = {
    var cpuRole = ""
    var memRole = ""
    var diskRole = ""
    var portsResource: RangesResource = null

    offerMatches(offer) match {
      case Some((cpu, mem, disk, ranges)) =>
        cpuRole = cpu
        memRole = mem
        diskRole = disk
        portsResource = ranges
      case _ =>
        log.info(
          s"No matching offer for ${app.id} (need cpus=${app.cpus}, mem=${app.mem}, " +
            s"disk=${app.disk}, ports=${app.hostPorts}) : " + offer
        )
        return None
    }

    val executor: Executor = if (app.executor == "") {
      Main.conf.executor
    }
    else {
      Executor.dispatch(app.executor)
    }

    val host: Option[String] = Some(offer.getHostname)

    val ports = portsResource.ranges.flatMap(_.asScala()).to[Seq]

    val taskId = newTaskId(app.id)
    val builder = TaskInfo.newBuilder
      // Use a valid hostname to make service discovery easier
      .setName(app.id.toHostname)
      .setTaskId(taskId)
      .setSlaveId(offer.getSlaveId)
      .addResources(ScalarResource(Resource.CPUS, app.cpus, cpuRole))
      .addResources(ScalarResource(Resource.MEM, app.mem, memRole))

    if (portsResource.ranges.nonEmpty) {
      builder.addResources(portsResource)
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

  //private
  def transformNeeds(need: Resource,
                     offered: Resource): Option[Resource] = {
    import scala.util.matching.Regex
    def nameMatched(a: Resource, b: Resource): Boolean = a match {
      case ScalarResource(aname, _, _) =>
        b match {
          case ScalarResource(bname, _, _) => (aname == bname)
          case RangesResource(bname, _, _) => (aname == bname)
          case SetResource(bname, _, _)    => (aname == bname)
        }
      case RangesResource(aname, _, _) =>
        b match {
          case ScalarResource(bname, _, _) => (aname == bname)
          case RangesResource(bname, _, _) => (aname == bname)
          case SetResource(bname, _, _)    => (aname == bname)
        }
      case SetResource(aname, _, _) =>
        b match {
          case ScalarResource(bname, _, _) => (aname == bname)
          case RangesResource(bname, _, _) => (aname == bname)
          case SetResource(bname, _, _)    => (aname == bname)
        }
    }

    def mkRegex(s: String): (Regex, Int) = {
      import java.util.regex.Pattern
      val regpat = "##r([0-9]*)##(.*)".r

      s match {
        case regpat(count, pat) =>
          val prefix = if (pat(0) != '^') "^" else ""
          val postfix = if (pat.last != '$') "$" else ""
          (new Regex(s"${prefix}${pat}${postfix}"), count.toInt)
        case _ =>
          val prefix = if (s(0) != '^') "^" else ""
          val postfix = if (s.last != '$') "$" else ""
          (new Regex(s"${prefix}${s}${postfix}"), 1)
      }
    }

    @annotation.tailrec
    def regSet(src: Set[String], dst: Set[String],
               selected: Set[String] = Set()): Option[Set[String]] = {
      if (src.isEmpty)
        Some(selected)
      else {
        val (re, count) = mkRegex(src.head)
        val cands =
          dst.filter(cand => !re.findFirstIn(cand).isEmpty).take(count)
        if (cands.size < count)
          None
        else
          regSet(src - src.head, dst -- cands, selected ++ cands)
      }
    }

    // TODO: Do we need to consider need.role for transformation?
    if (!nameMatched(need, offered))
      None
    else
      need match {
        case ScalarResource(name, value, role) =>
          offered match {
            case ScalarResource(_, valueOffered, roleOffered) =>
              if (value <= valueOffered)
                Some(ScalarResource(name, value, roleOffered))
              else
                None
            case RangesResource(_, rangesOffered, roleOffered) =>
              // input: scalar 90.2
              // offered: ranges of (1 - 100), (200 - 300)
              // request: ranges of (90 - 91)
              rangesOffered.find { r =>
                if (r.begin <= value.floor && r.end >= value.ceil)
                  true
                else
                  false
              } match {
                case Some(_) =>
                  Some(RangesResource(name,
                    Seq(Range(value.floor.toLong,
                      value.ceil.toLong)),
                    roleOffered))
                case None => None
              }

            case SetResource(_, itemsOffered, roleOffered) =>
              // input: scalar 2.4 (truncated to 3)
              // offered: set of "foo", "bar", "car"
              // request: set of "foo", "bar"
              val took = itemsOffered.take(value.ceil.toInt)
              if (took.size == value.ceil.toInt)
                Some(SetResource(name, took, roleOffered))
              else
                None
          }
        case RangesResource(name, ranges, role) =>
          offered match {
            case ScalarResource(_, valueOffered, roleOffered) => None
            case RangesResource(_, rangesOffered, roleOffered) =>
              val satisfied = for (
                s <- ranges;
                if !rangesOffered.find {
                  r => (s.begin >= r.begin && s.end <= r.end)
                }.isEmpty
              ) yield s
              if (ranges.size == satisfied.size)
                Some(RangesResource(name, ranges, roleOffered))
              else
                None
            case SetResource(_, itemsOffered, roleOffered) => None
          }
        case SetResource(name, items, role) =>
          offered match {
            case ScalarResource(_, valueOffered, roleOffered)  => None
            case RangesResource(_, rangesOffered, roleOffered) => None
            case SetResource(_, itemsOffered, roleOffered) =>
              regSet(items, itemsOffered) match {
                case Some(selected) =>
                  Some(SetResource(name, selected, roleOffered))
                case None =>
                  None
              }
          }
      }
  }

  private def offerMatches(offer: Offer): Option[(String, String, String, RangesResource)] = {
    var cpuRole = ""
    var memRole = ""
    var diskRole = ""
    val portMatcher = new PortsMatcher(app, offer)

    for (resource <- offer.getResourcesList.asScala) {
      if (cpuRole.isEmpty &&
        resource.getName == Resource.CPUS &&
        resource.getScalar.getValue >= app.cpus) {
        cpuRole = resource.getRole
      }
      if (memRole.isEmpty &&
        resource.getName == Resource.MEM &&
        resource.getScalar.getValue >= app.mem) {
        memRole = resource.getRole
      }
      if (diskRole.isEmpty &&
        resource.getName == Resource.DISK &&
        resource.getScalar.getValue >= app.disk) {
        diskRole = resource.getRole
      }
    }

    if (cpuRole.isEmpty || memRole.isEmpty || diskRole.isEmpty) {
      return None
    }

    val badConstraints: Set[Constraint] = {
      val runningTasks = taskTracker.get(app.id)
      app.constraints.filterNot { constraint =>
        Constraints.meetsConstraint(runningTasks, offer, constraint)
      }
    }

    portMatcher.portRanges match {
      case None =>
        log.warn("App ports are not available in the offer.")
        None

      case _ if badConstraints.nonEmpty =>
        log.warn(
          s"Offer did not satisfy constraints for app [${app.id}].\n" +
            s"Conflicting constraints are: [${badConstraints.mkString(", ")}]"
        )
        None

      case Some(portRanges) =>
        log.debug("Met all constraints.")
        Some((cpuRole, memRole, diskRole, portRanges))
    }
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
