package messages

import akka.actor.ActorRef

case class Register(list : List[ActorRef], lstType : String)