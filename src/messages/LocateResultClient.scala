package messages

import akka.actor.ActorRef

case class LocateResultClient (servers: List[ActorRef], stat: ActorRef)