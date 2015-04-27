package mesosphere.marathon.tasks

import scala.util.{ Try, Random }
import org.apache.mesos.Protos.Offer
import mesosphere.mesos.protos._
import mesosphere.marathon.Protos.Constraint
import mesosphere.marathon.PortResourceException
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.state.Container
import mesosphere.marathon.state.Container.Docker.PortMapping
import mesosphere.util.Logging
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object ResourceMatcher {

  def getName(r: Resource): String = r match {
    case ScalarResource(name, _, _) => name
    case RangesResource(name, _, _) => name
    case SetResource(name, _, _)    => name
  }
  def getRole(r: Resource): String = r match {
    case ScalarResource(_, _, role) => role
    case RangesResource(_, _, role) => role
    case SetResource(_, _, role)    => role
  }
}

class ResourceMatcher(app: AppDefinition, offer: Offer) extends Logging {

  def doMatch(needed: Seq[Resource], offered: Seq[Resource],
              result: Seq[Resource] = Seq()): Option[Seq[Resource]] = {
    needed match {
      case resNeeded :: tl =>
        val name = ResourceMatcher.getName(resNeeded)
        var need: Option[Resource] = None

        val remains = offered.filter { o =>
          if (need.isEmpty) {
            if (name == Resource.PORTS) {
              val portMatcher = new PortsMatcher(app, offer)
              portMatcher.portRanges match {
                case x @ Some(_) =>
                  need = x
                  false
                case None =>
                  log.warn("App ports are not available in the offer.")
                  true
              }
            }
            else
              transformNeeds(resNeeded, o) match {
                case x @ Some(_) =>
                  need = x; false
                case _ => true
              }
          }
          else
            true
        }

        need match {
          case Some(x) => doMatch(tl, remains, result ++ Seq(x))
          case _       => None
        }

      case Nil if !result.isEmpty => Some(result)

      case _                      => None
    }
  }

  def normalizeReqs(needed: Seq[Resource]): Seq[Resource] = {
    import mesosphere.mesos.protos._
    var added: Seq[Resource] = Seq()

    needed.find(r => ResourceMatcher.getName(r) == Resource.PORTS) match {
      case None => added = added ++ Seq(RangesResource(Resource.PORTS, Seq(Range(0, 0))))
      case _    =>
    }
    needed.find(r => ResourceMatcher.getName(r) == Resource.CPUS) match {
      case None => added = added ++ Seq(ScalarResource(Resource.CPUS, AppDefinition.DefaultCpus))
      case _    =>
    }
    needed.find(r => ResourceMatcher.getName(r) == Resource.MEM) match {
      case None => added = added ++ Seq(ScalarResource(Resource.PORTS, AppDefinition.DefaultMem))
      case _    =>
    }
    needed ++ added
  }

  def resources(needed: Seq[Resource] = app.resources): Option[Seq[Resource]] = {
    import mesosphere.mesos.protos.Implicits._
    val offers = for (r <- offer.getResourcesList().asScala.toList.toSeq)
      yield implicitly[Resource](r)

    doMatch(normalizeReqs(needed), offers)
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
              if (took.size == value.ceil.toInt) {
                Some(SetResource(name, took, roleOffered))
              }
              else {
                None
              }
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
}
