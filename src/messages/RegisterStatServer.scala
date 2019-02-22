package messages

import akka.actor.ActorRef

case class RegisterStatServer(stat : ActorRef)