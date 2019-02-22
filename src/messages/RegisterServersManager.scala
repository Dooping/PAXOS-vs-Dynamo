package messages

import akka.actor.ActorRef

case class RegisterServersManager(manager: ActorRef)