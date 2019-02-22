package messages

import akka.actor.ActorRef

case class LocateResult(servers: List[ActorRef], lstType: String)