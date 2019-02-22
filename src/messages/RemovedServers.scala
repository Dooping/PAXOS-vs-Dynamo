package messages

import akka.actor.ActorRef

case class RemovedServers(servers: List[ActorRef], algType: String)