package states

import akka.actor.Actor
import akka.actor.ActorRef
import collection.mutable.HashMap
import crdt._

class ProxySt {
  
  private case class OpInfo(operation: String, client: ActorRef, quorum: List[ActorRef], getResultsRec: List[ReplicaSt])
  
  //operationId => (client,quorum,nGetResults) 
  private var opMap = HashMap[Int,OpInfo]()
  
  def initOp(opId: Int, operation: String, client: ActorRef, quorum: List[ActorRef]) = {
    if(!opMap.contains(opId))
      opMap += (opId -> new OpInfo(operation,client,quorum,List[ReplicaSt]()))
  }
  
  //Get the number of messages GetResults already received, for this operation with opId
  def getNumGetResults(opId: Int) : Int = {
    if(opMap.contains(opId))
        return opMap(opId).getResultsRec.size
    
    return -1
  }
  
  def getQuorumSize(opId: Int) : Int = {
    if(opMap.contains(opId))
      return opMap(opId).quorum.size
    
    return -1
  }
  
  def getQuorum(opId: Int) : List[ActorRef] = {
    var res = List[ActorRef]()
    if(opMap.contains(opId))
      res =  opMap(opId).quorum
    
    return res
  }
  
  def getClient(opId: Int) : ActorRef = {
    if(opMap.contains(opId))
      return opMap(opId).client
      
    return null
  }
  
  def addGetResultRec(opId: Int, context: List[(String,Int)], values: List[String], sender: ActorRef) = {
    if(opMap.contains(opId)){
      val oldOpInfo = opMap(opId)
      
      //If the answer came from a server belonging to the quorum
      if(oldOpInfo.quorum.contains(sender)){
        var auxLst = oldOpInfo.getResultsRec
      
        var auxRepSt = new ReplicaSt()
        auxRepSt.setContext(context)
        auxRepSt.setValues(values)
      
        auxLst ::= auxRepSt
      
        opMap += (opId -> new OpInfo(oldOpInfo.operation, oldOpInfo.client, oldOpInfo.quorum, auxLst))
      }
      
    }
  }
  
  def getOperation(opId: Int) : String = {
    if(opMap.contains(opId))
      return opMap(opId).operation
      
    return ""
  }
  
  //Return context,values
  def combineGetResults(opId: Int) : (List[(String,Int)], List[String]) = {
    if(opMap.contains(opId)){
      val auxLst = opMap(opId).getResultsRec
      
      var higherVectorClock = List[(String,Int)]()
      var auxRet = (List[(String,Int)](),0)
      var chosenIndex = -1
      var auxValuesVersions = List[List[String]]()
      
      for(i <- 0 to auxLst.size-1){
        auxRet = compareVectorClocks(higherVectorClock,auxLst(i).getContext())
        
        if(auxRet._2 == 2){//Higher clock = parameter 2
          //Store the index where the clock was higher
          chosenIndex = i
        }else if(auxRet._2 == 3){//Higher clock = combination of both parameters
          
          if(!higherVectorClock.isEmpty){
            //Reset this index in order to indicate, that there will be the need to combine the values
            chosenIndex = -1
          }else{
            //It will be empty the first time, which will result in being a combination of clocks since the size of both lists were different
            chosenIndex = i
          }
            
          
        }
        
        higherVectorClock = auxRet._1
        auxValuesVersions ::= auxLst(i).getValues()
      }
      
      //Combine values
      if(chosenIndex == -1){
        return (higherVectorClock,combineValues(auxValuesVersions))
      }else{//Get the value on the chosenIndex
        return  (higherVectorClock,auxLst(chosenIndex).getValues())
      }
    }
    
    return null
  }
  
  //This will compare clock1 with clock2 and return a new version of the clock, either being one of the 2 clocks or a new combination one
  //The second argument of the tuple will indicate which one of the clock was higher. 3=combination one
  def compareVectorClocks(clock1: List[(String,Int)], clock2: List[(String,Int)]) : (List[(String,Int)],Int) = {
    var res = List[(String,Int)]()
    var higherClock = 3
    var auxElem = Option[(String,Int)]("",-1)
    
    var nTimesHigherOrEqual = 0
    var nTimesLower = 0
    
    if(clock1.size >= clock2.size){
      for(c <- clock1){
        auxElem = clock2.find(elem => elem._1.equals(c._1))
        
        if(auxElem != None){
          if(c._2 >= auxElem.get._2){
            res ::= c
            nTimesHigherOrEqual += 1
          }else{
            res ::= auxElem.get
            nTimesLower += 1
          }
        }else
          res ::= c
      }
      
      
      if(nTimesHigherOrEqual == clock1.size){//Clock 1 > Clock 2 
        higherClock = 1
      }else if(nTimesLower == clock1.size){//Clock 1 < Clock 2 
        higherClock = 2
      }else{//Clock 1, Clock 2 are incomparable since there were concurrent writes
        higherClock = 3
      }
      
    }else if(clock1.size < clock2.size){
      for(c <- clock2){
        auxElem = clock1.find(elem => elem._1.equals(c._1))
        
        if(auxElem != None){
          if(c._2 >= auxElem.get._2){
            res ::= c
            nTimesHigherOrEqual += 1
          }else{
            res ::= auxElem.get
            nTimesLower += 1
          }
        }else
          res ::= c
      }
      
      
      if(nTimesHigherOrEqual == clock2.size){//Clock 2 > Clock 1 
        higherClock = 2
      }else if(nTimesLower == clock2.size){//Clock 2 < Clock 1 
        higherClock = 1
      }else{//Clock 1, Clock 2 are incomparable since there were concurrent writes
        higherClock = 3
      }
    }
    
    return (res,higherClock)
  }
  
  //Return a new combination of values
  def combineValues(valuesVersions: List[List[String]]) : List[String] = {    
    var resCrdt = new CRDT()
    
    for(lst <- valuesVersions){
      for(v <- lst)
        resCrdt.add(v)
    }
    
    return resCrdt.list()
  }
  
  def getAllOpsIds() : Set[Int] = {
    var auxSet = Set[Int]()
    auxSet ++= opMap.keySet
    
    return auxSet
  }
  
  //Return the new servers of the quorum
  def refreshStateOp(newSetServers: Set[ActorRef], opId: Int): Set[ActorRef] = {
    var newQuorum = Set[ActorRef]()
    
    var nToAdd = 0
    var newElems = Set[ActorRef]()
    
    val oldOpInfo = opMap(opId)
    
    newQuorum = oldOpInfo.quorum.toSet.filter { elem => newSetServers.contains(elem) }
    
    nToAdd = oldOpInfo.quorum.size - newQuorum.size
      
    newElems = chooseNewServer(newSetServers, newQuorum, nToAdd)
      
    //The number of elements in the new quorum is satisfied
    if(newElems.size == nToAdd){
      newQuorum ++= newElems
      
      opMap += (opId -> new OpInfo(oldOpInfo.operation, oldOpInfo.client, newQuorum.toList, oldOpInfo.getResultsRec))
      
      return newElems
    }
    
    return Set[ActorRef]()
    
  }
  
  private def chooseNewServer(fromSet: Set[ActorRef], excludeSet: Set[ActorRef], nToChoose: Int) : Set[ActorRef] = {
    var res = Set[ActorRef]()
    val nElemsLeft = nToChoose
    
    res = fromSet.filterNot {excludeSet}
   
    if(res.size > nToChoose){
      return res.drop(res.size-nToChoose)
    }

    return res
  }
  
  def removeOp(opId: Int) : Unit = {
    opMap.remove(opId)
  }
  
}