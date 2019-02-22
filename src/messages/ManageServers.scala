package messages

import akka.actor.ActorRef

case class ManageServers(serversPaxos: List[ActorRef], serversDynamo: List[ActorRef])