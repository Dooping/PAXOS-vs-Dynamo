package states

import akka.actor.ActorRef
import collection.mutable.HashMap

class LearnerSt() {
  
  private val OP_INSERT = "insert"
  private val OP_REMOVE = "remove"
  
  private var decision = ""
  
  private var na = -1
  
  private var va = ""
  
  private var decElements = Set[String]()
  
  private var opsQuorums = HashMap[String,Set[ActorRef]]()
  
  def addActor(op: String, ac: ActorRef) = {
    if(opsQuorums.contains(op))
    {
      var aset = opsQuorums(op)
      aset += ac
      opsQuorums.update(op, aset)
    }
  }
  
  def size(op: String):Int = {
    if(opsQuorums.contains(op))
      return opsQuorums(op).size
    else
      return 0
  }
  
  def getDecision() = decElements.toList
  
  def setDecision(op: String) = { 
    var value = ""
    if(op.contains(OP_INSERT))
    {
      value = op.stripPrefix(OP_INSERT+"(")
      value = value.stripSuffix(")")
      decElements += value
    }else if(op.contains(OP_REMOVE)){
      value = op.stripPrefix(OP_REMOVE+"(")
      value = value.stripSuffix(")")
      decElements -= value
    }
  }
  
  def getNa() = na
  
  def setNa(n: Int) = { na = n }
  
  def getVa() = va
  
  def setVa(v: String) = { va = v }
  
  def addOp(op: String) = {
		  if(!opsQuorums.contains(op))
		    opsQuorums += (op -> Set[ActorRef]())
  }
  
  private def setContent(decision: String, na: Int, va: String, decElements: Set[String], opsQuorums: HashMap[String,Set[ActorRef]]) = {
    this.decision = decision
    this.na = na
    this.va = va
    this.decElements = decElements
    
    var auxMap = new HashMap[String,Set[ActorRef]]()
    
    for(pair <- opsQuorums){
      auxMap += new Tuple2(pair._1,pair._2)
    }
    this.opsQuorums = auxMap
  }
  
  def newCopy(): LearnerSt = {
    var res = new LearnerSt()
    var aux1 = this.decision
    var aux2 = this.na
    var aux3 = this.va
    var aux4 = this.decElements
    var aux5 = this.opsQuorums
    res.setContent(aux1, aux2,aux3, aux4, aux5)
    return res
  }
  
}