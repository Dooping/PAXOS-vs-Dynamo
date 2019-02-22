package actors

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import actors._
import messages._

class Root() extends Actor {
   
 val paxosType = "Paxos"
 val dynamoType = "Dynamo"
  
 var numServers =3
 var numClients = 3
 var numOps=1000
 var numKeys =100
 var insPerc = 0.4
 var remPerc= 0.1
 var isElemPerc = 0.4
 var listPerc= 0.1
 var dynamicChangeList =List[(Int, Int , Int)]()

 for(line <- scala.io.Source.fromFile("config.cfg").getLines()){
	 val segments= line.split(":")
			 if(segments(0).toLowerCase().contains("number of servers")){
				 numServers = segments(1).toInt
			 }else if(segments(0).toLowerCase().contains("number of clients")){
				 numClients = segments(1).toInt
			 }else if(segments(0).toLowerCase().contains("number of operations")){
				 numOps = segments(1).toInt
			 }else if(segments(0).toLowerCase().contains("number of keys")){
				 numKeys = segments(1).toInt
			 }else if(segments(0).toLowerCase().contains("insert percentage")){
				 insPerc = segments(1).toDouble
			 }else if(segments(0).toLowerCase().contains("remove percentage")){
				 remPerc = segments(1).toDouble
			 }else if(segments(0).toLowerCase().contains("iselement percentage")){
				 isElemPerc = segments(1).toDouble
			 }else if(segments(0).toLowerCase().contains("list percentage")){
				 listPerc = segments(1).toDouble
			 }else if(segments(0).toLowerCase().contains("dynamic change")){
			   var vals= segments(1).split(",")
				 dynamicChangeList::=((vals(0).toInt,vals(1).toInt,vals(2).toInt))
			 }
   }
        
 var serverListPaxos = List[ActorRef]()
 var serverListDynamo = List[ActorRef]()
 var clientListPaxos = List[ActorRef]()
 var clientListDynamo = List[ActorRef]()
 
 
 var proxiesList = List[ActorRef]()
 
 var paxosCounter = 1
 var dynamoCounter = 1
 
 def receive = {
    case Create() => {
      
      for(i <- 1 to numServers) {
        var server = context.actorOf(Props(new Server(paxosType)), "serverPaxos"+paxosCounter)
        serverListPaxos ::= server
        paxosCounter += 1
        server = context.actorOf(Props(new Server(dynamoType)), "serverDynamo"+dynamoCounter)
        serverListDynamo ::= server
        dynamoCounter += 1
      }
    
      var proxy = context.actorOf(Props[Proxy], "proxy")
      proxiesList ::= proxy
    
      var locator = context.actorOf(Props(new Locator(serverListPaxos,serverListDynamo,proxiesList)), "locator")
    
      var statistics = context.actorOf(Props(new Statistics(locator)), "statistics")
    
    
      for(i <- 1 to numClients) {
        var client = context.actorOf(Props(new Client(paxosType,locator, numOps,numKeys,insPerc,remPerc,isElemPerc,listPerc)), "clientPaxos"+i)
        clientListPaxos ::= client
        client = context.actorOf(Props(new Client(dynamoType,locator, numOps,numKeys,insPerc,remPerc,isElemPerc,listPerc)), "clientDynamo"+i)
        clientListDynamo ::= client 
      }
      
      var serversManager = context.actorOf(Props(new ServersManager(locator, dynamicChangeList.reverse)), "serversManager")
    
    }
    case CreateServers(nServersToAdd, this.paxosType) => {
      var auxLst = List[ActorRef]()
      
      for(i<-1 to nServersToAdd){
        var auxServ = context.actorOf(Props(new Server(this.paxosType)), "serverPaxos"+paxosCounter)
        auxLst ::= auxServ
        paxosCounter += 1
      }
         
      serverListPaxos ++= auxLst
      sender ! CreatedServers(auxLst,this.paxosType)
    }
    case CreateServers(nServersToAdd, this.dynamoType) => {
      var auxLst = List[ActorRef]()
      for(i<-1 to nServersToAdd){
        var auxServ = context.actorOf(Props(new Server(this.dynamoType)), "serverDynamo"+dynamoCounter)
        auxLst ::= auxServ
        dynamoCounter += 1
      }
      
      serverListDynamo ++= auxLst
      sender ! CreatedServers(auxLst,this.dynamoType)
    }
    case RemoveServers(servers,this.paxosType) => {
      serverListPaxos = serverListPaxos.filter { elem => !servers.contains(elem) }
        
      sender ! RemovedServers(servers,this.paxosType)
    }
    case RemoveServers(servers,this.dynamoType) => {
      serverListDynamo = serverListDynamo.filter { elem => !servers.contains(elem) }
        
      sender ! RemovedServers(servers,this.dynamoType)
    }
    
}
 
}