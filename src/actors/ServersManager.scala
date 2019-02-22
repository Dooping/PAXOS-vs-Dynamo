package actors

import akka.actor.Actor
import akka.actor.ActorRef
import messages._
import scala.util.Random

class ServersManager(locator: ActorRef, dynamicChangeList : List[(Int, Int , Int)]) extends Actor {
  
  val paxosType = "Paxos"
  val dynamoType = "Dynamo"
  
  var serversPaxos = List[ActorRef]()
  var serversDynamo = List[ActorRef]()
  
  def receive = {
    case CreatedServers(serverLst,this.paxosType) => {
      addServers(serverLst,this.paxosType)
    }
    case CreatedServers(serverLst,this.dynamoType) => {
      addServers(serverLst,this.dynamoType)
    }
    case RemovedServers(serverLst,this.paxosType) => {
      removeServers(serverLst,this.paxosType)
    }
    case RemovedServers(serverLst,this.dynamoType) => {
      removeServers(serverLst,this.dynamoType)
    }
    case ManageServers(sPaxos,sDynamo) => {
      serversPaxos = sPaxos
      serversDynamo = sDynamo
      for(c <- dynamicChangeList){
        /* This first dynamic change over paxos does not always produce the right behavior
         * resulting in the clients not being able to finish
         * For proper functioning of the program, it's advisable to leave this line commented
         * */
        //dynamicChangeEnv(c._1,c._2,c._3,this.paxosType)
        dynamicChangeEnv(c._1,c._2,c._3,this.dynamoType)
      }
    }
  }
  
  locator ! RegisterServersManager(self)
  
  def addServers(serverLst: List[ActorRef], algType: String) = {
    locator ! ChangeServer("add", serverLst, algType)
  }
  
  def removeServers(serverLst: List[ActorRef], algType: String) = {
    locator ! ChangeServer("remove", serverLst, algType)
  }
  
  def changeRepNum(kRep: Int, algType: String) = {
    locator ! ChangeKRep(kRep,algType)
  }
  
  private def verifyChangeEnvRequest(nServersToAdd: Int, nServersToRemove: Int, kRep: Int, algType: String): Boolean = {
    var canChangeEnv = false
    var auxPrint = "For Dynamic Change:"+nServersToAdd+","+nServersToRemove+","+kRep+'\n'
    
    if(nServersToAdd < 0){
      auxPrint += "Invalid number of servers to add " + nServersToAdd+'\n'
    }
    
    if(nServersToRemove < 0){
      auxPrint += "Invalid number of servers to remove " + nServersToRemove+'\n'
    }
    
    if(kRep >= (2*nServersToRemove + 1)){
      
      if(algType.equals(paxosType)){
        
        //Number of replicas can not be higher than the number of servers
        if(nServersToAdd + serversPaxos.size >= kRep){
          
          //Number of replicas can not be lower than the difference between the number of existing servers and 
          //the number of servers that are going to fail
          if(serversPaxos.size - nServersToRemove >= kRep){
            canChangeEnv = true
          }else{
            auxPrint += "kRep value = " + kRep + " is lower thant the final number of paxos servers " + (serversPaxos.size - nServersToRemove)+'\n'
          }
          
        }else{
          auxPrint += "kRep value = " + kRep + " is higher than the number of paxos servers " + (nServersToAdd + serversPaxos.size)+'\n'
        }
        
      }else if(algType.equals(dynamoType)){
        
        //Number of replicas can not be higher than the number of servers
        if(nServersToAdd + serversDynamo.size >= kRep){
          
          //Number of replicas can not be lower than the difference between the number of existing servers and 
          //the number of servers that are going to fail
          if(serversDynamo.size - nServersToRemove >= kRep){
            canChangeEnv = true
          }else{
            auxPrint += "kRep value = " + kRep + " is lower thant the final number of dynamo servers " + (serversDynamo.size - nServersToRemove)+'\n'
          }
          
        }else{
          auxPrint += "kRep value = " + kRep + " is higher than the number of dynamo servers " + nServersToAdd + serversDynamo.size+'\n'
        }
        
      }
      
    }else{
      auxPrint += "kRep value = " + kRep + " is too low to support proper functioning of the system"+'\n'
    }
    
    if(!canChangeEnv)
      println(auxPrint)
    return canChangeEnv
  }
  
  def dynamicChangeEnv(nServersToAdd: Int, nServersToRemove: Int, kRep: Int, algType: String) = {
    
    
    var canChangeEnv = verifyChangeEnvRequest(nServersToAdd, nServersToRemove, kRep, algType)
    
    
    if(canChangeEnv){
      context.parent ! CreateServers(nServersToAdd,algType)
          
      val r = new Random(System.nanoTime())
    
      var idx = 0
    
      var auxLst = List[ActorRef]()
    
      for(i <- 1 to nServersToRemove){
        if(algType.equals(paxosType)){
          if(serversPaxos.size > 0)
            auxLst ::= serversPaxos(r.nextInt(serversPaxos.size))
        }else if(algType.equals(dynamoType)){
          if(serversDynamo.size > 0)
            auxLst ::= serversDynamo(r.nextInt(serversDynamo.size))
        }
        
      }
    
      context.parent ! RemoveServers(auxLst,algType)
    
      changeRepNum(kRep,algType)
    }
    
  }
}