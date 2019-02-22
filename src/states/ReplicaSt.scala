package states

import akka.actor.Actor
import akka.actor.ActorRef

class ReplicaSt {
  
  private var values = List[String]()
  
  /* It is a list because the number o replicas can vary.
   * Tuples are used so we can know what server is related to each Int, ex: [(S1,0),(S2,0),(S3,0),...]
  */
  private var context = List[(String,Int)]()
  
  //It's called when the server receives a put
  def updateState(newValues: List[String], newContext: List[(String,Int)], nodeId: String) = {
    this.values = newValues
    this.context = newContext
    
    var i = 0
    var newVal = 0
    while(i < context.size){
      if(context(i)._1.equals(nodeId)){
        //Get the old value and increment it by one
        newVal = context(i)._2 + 1
        
        i = context.size
        context = context.filter(elem => !elem._1.equals(nodeId))
      }
        
      i += 1
    }
    
    context ::= (nodeId,newVal)
    
  }
  
  def getContext() = context
  
  def setContext(newContext: List[(String,Int)]) = {
    this.context = newContext
  }
  
  def getValues() = values
  
  def setValues(newValues: List[String]) = {
    this.values = newValues
  }
  
  def initContext(servers: Set[ActorRef]) = {
    for(s <- servers){
      context ::= (s.path.name,0)
    }
  }
  
  private def setContent(context : List[(String,Int)], values : List[String]) = {
    this.context = context
    this.values = values
  }
  
  def newCopy(): ReplicaSt = {
    var res = new ReplicaSt()
    var aux1 = this.context
    var aux2 = this.values
    res.setContent(aux1, aux2)
    return res
  }
}