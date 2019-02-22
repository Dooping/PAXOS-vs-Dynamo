package actors

import akka.actor.Actor
import akka.actor.ActorRef
import java.util.Random
import messages._
import collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

class Client(algorithmType: String, locator: ActorRef, numOps: Integer, nKeys: Integer, insPerc: Double, remPerc: Double, isElemPerc: Double, listPerc: Double) extends Actor {
  
  val N_IDS = 64
  
  var servers = List[ActorRef]()
  var count = 1
  var statServer = Actor.noSender
  var initTime = System.nanoTime()
  
  var lastKeySent = ""
  
  var writeTimes = new ListBuffer[Double]()
  var readTimes = new ListBuffer[Double]()
  
  var nReads = 0
  var nReadsReceived = 0
  var nWrites = 0
  var nInsertLeft = (numOps*insPerc).toInt
  var nRemoveLeft = (numOps*remPerc).toInt
  var nIsElemLeft = (numOps*isElemPerc).toInt
  var nListLeft = (numOps*listPerc).toInt

  
  val random = new Random(System.nanoTime())
  
  def receive = {
    case LocateResultClient(s,stat) => {
      servers = s
      statServer = stat
      
      val aux = decideWriteOp()
      write(aux._1, aux._2,algorithmType)
    }
    case IsElementResult(k,res,opId) => {
      if(!k.equals("-1")){
        //Calculate read time
        val keyNum = k.substring(4).toInt
        readTimes(nReadsReceived) = (System.nanoTime() - readTimes(nReadsReceived))/1000000000
      }
      
      nReadsReceived += 1
      
      if(count == numOps+1 && k.equals(lastKeySent))
      {
        statServer ! Stats((System.nanoTime() - initTime*1.0)/1000000000, numOps, writeTimes.toList, readTimes.toList, algorithmType)
      }
    }
    case ListResult(k,res,opId) => {
      if(!k.equals("-1")){
        //Calculate read time
        val keyNum = k.substring(4).toInt
        readTimes(nReadsReceived) = (System.nanoTime() - readTimes(nReadsReceived))/1000000000
      }
      
      nReadsReceived += 1
      
      if(count == numOps+1 && k.equals(lastKeySent))
      {
        statServer ! Stats((System.nanoTime() - initTime*1.0)/1000000000, numOps, writeTimes.toList, readTimes.toList, algorithmType)
      }
    }
    case Decided(k,opId) => {
      
      if(!k.equals("-1")){
        //Calculate write time
        val keyNum = k.substring(4).toInt
        writeTimes(nWrites-1) = (System.nanoTime() - writeTimes(nWrites-1))/1000000000
      }
      
      var aux = decideReadOp()
      read(aux._1, aux._2,algorithmType)
      if(count < numOps)
      {
        aux = decideWriteOp()
        write(aux._1, aux._2,algorithmType)
      }else{
        lastKeySent = aux._1
      }
    }
  }
  
  locator ! LocateProxy()
  
  def write(k: String, op: String, algType: String) = {
    nWrites += 1
    val keyNum = k.substring(4).toInt
    writeTimes += System.nanoTime()
    servers(keyNum%servers.size) ! Write(k,op,algType)
  }
  
  def read(k: String, op: String, algType: String) = {
    nReads += 1
    val keyNum = k.substring(4).toInt
    readTimes += System.nanoTime()
    servers(keyNum%servers.size) ! Read(k,op,algType)
  }
  
  def decideWriteOp() : (String,String) = {
    var opChosen = ""
    
    val elem = "elem_"+ (random.nextInt(nKeys) + 1)
    val k = "key_"+ (random.nextInt(nKeys) + 1)
    
    val opts = nInsertLeft + nRemoveLeft
     
    if(opts > 0){
    	val opInd = random.nextInt(opts)

      if(opInd < nInsertLeft){//Insert
        opChosen = "insert("+elem+")"
        nInsertLeft -= 1
      }else{//Remove
        opChosen = "remove("+elem+")"
        nRemoveLeft -= 1
      }
    }
    count += 1
    return (k,opChosen)
  }
  
  def decideReadOp() : (String,String) = {
    var opChosen = ""
    
    val elem = "elem_"+ (random.nextInt(nKeys) + 1)
    val k = "key_"+ (random.nextInt(nKeys) + 1)
    
    val opts = nIsElemLeft + nListLeft
     
    if(opts > 0){
    	val opInd = random.nextInt(opts)

      if(opInd < nIsElemLeft){//isElement
        opChosen = "isElement("+elem+")"
        nIsElemLeft -= 1
      }else{//list
        opChosen = "list()"
        nListLeft -= 1
      }
    }
    count += 1
    return (k,opChosen)
  }
  
}