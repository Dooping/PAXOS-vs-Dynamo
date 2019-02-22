package messages

import akka.actor.ActorRef

case class CreatedServers(servers: List[ActorRef], algType: String)