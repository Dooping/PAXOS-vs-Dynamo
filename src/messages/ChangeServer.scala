package messages

import akka.actor.ActorRef

case class ChangeServer(op: String, servers: List[ActorRef], algType: String)