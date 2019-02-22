package messages

import akka.actor.Actor
import akka.actor.ActorRef

case class RemoveServers(servers: List[ActorRef], algType: String)