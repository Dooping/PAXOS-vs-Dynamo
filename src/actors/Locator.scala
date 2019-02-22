package actors

import akka.actor.Actor
import akka.actor.ActorRef
import messages._

class Locator(listPaxos: List[ActorRef], listDynamo: List[ActorRef], listProxies: List[ActorRef]) extends Actor {
  
  val paxosType = "Paxos"
  val dynamoType = "Dynamo"
  
  var serversPaxos = listPaxos
  var serversDynamo = listDynamo
  var proxies = listProxies
  
  var nextLeader = 0
  var statServer = self
  var serversManager = self
  var pendingRequesters = List[ActorRef]()
  var clients = Set[ActorRef]()
  
  def receive = {
    case Register(list, this.paxosType) => {
        serversPaxos = list
    }
    case Register(list, this.dynamoType) => {
        serversDynamo = list
    }
    case RegisterStatServer(stat) => {
        statServer = stat
        pendingRequesters.foreach { 
          c => c ! LocateResultClient(proxies, statServer)
        }
        pendingRequesters = List[ActorRef]()
    }
    case RegisterServersManager(manager) => {
      serversManager = manager
    }
    case ManageServers(sPaxos,sDynamo) => {
      if(!serversManager.equals(self))
        serversManager ! ManageServers(sPaxos,sDynamo)
    }
    case LocateProxy() => {
      clients += sender
      if(!statServer.equals(self))
        sender ! LocateResultClient(proxies,statServer)
      else
        pendingRequesters ::= sender
    }
    case ChangeServer("add", servers, lstType) => {
        if(lstType.equals(paxosType)){  
          serversPaxos ++= servers
          serversPaxos.foreach { s => s ! Register(serversPaxos, lstType) }
          proxies.foreach { p => p ! Register(serversPaxos, lstType) }
        }else if(lstType.equals(dynamoType)){
        	serversDynamo ++= servers
        	serversDynamo.foreach { s => s ! Register(serversDynamo, lstType) }
        	proxies.foreach { p => p ! Register(serversDynamo, lstType) }
        }
    }
    case ChangeServer("delete", servers, lstType) => {
        if(lstType.equals(paxosType)){  
          serversPaxos = serversPaxos.filter(elem => !servers.contains(elem))
          serversPaxos.foreach { s => s ! Register(serversPaxos, lstType) }
          proxies.foreach { p => p ! Register(serversPaxos, lstType) }
        }else if(lstType.equals(dynamoType)){
        	serversDynamo = serversDynamo.filter(elem => !servers.contains(elem))
        	serversDynamo.foreach { s => s ! Register(serversDynamo, lstType) }
        	proxies.foreach { p => p ! Register(serversDynamo, lstType) }
        }
    }
    case ChangeKRep(kRep,algType) => {
      if(algType.equals(paxosType)){
          proxies.foreach { p => p ! ChangeKRep(kRep, algType) }
          serversPaxos.foreach { s => s ! ChangeKRep(kRep, algType) }
        }else if(algType.equals(dynamoType)){
          proxies.foreach { p => p !ChangeKRep(kRep, algType) }
        	serversDynamo.foreach { s => s ! ChangeKRep(kRep, algType) }
        }
    }
  }
  
  for(server <- serversPaxos) server ! Register(serversPaxos, paxosType)
  for(server <- serversDynamo) server ! Register(serversDynamo, dynamoType)
  for(server <- proxies){ 
    server ! Register(serversPaxos, paxosType)
    server ! Register(serversDynamo, dynamoType)
  }
}