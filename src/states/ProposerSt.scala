package states

import akka.actor.ActorRef

class ProposerSt() {
  
  private var vaLst = List[String]()
  private var naLst = List[Int]()
  private var v = ""
  private var acceptOkRec = 0
  private var clientSet = Set[ActorRef]()
  private var opIds = Set[Int]()
  private var n = -1
  
  
  def addVa(na: Int, va: String) = { 
    naLst ::= na 
    vaLst ::= va
  }
  
  def size() = naLst.size
  
  def getHighestVa(): String = {
    var n = -1
    var va = ""
    
    for(i <- 0 to naLst.size-1)
      if (naLst(i)>n){
        n = naLst(i)
        va = vaLst(i)
      }
      
    return va
  }
  
  def getN() = n
  
  def setN(nRec: Int) = {n = nRec}
  
  def getV() = v
  
  def setV(va: String) = {v = va}
  
  def getAcceptOkRec() = acceptOkRec
  
  def incAcceptOkRec() = {acceptOkRec += 1}
  
  def addClient(cl: ActorRef, opId: Int) = { 
    clientSet += cl 
    opIds += opId
  }
  
  def getClientSet() = clientSet
  
  def getOpIds() = opIds
  
  def remClients() = { 
    clientSet = clientSet.empty 
    opIds = opIds.empty
  }
  
  private def setContent(vaLst : List[String], naLst : List[Int], v : String, acceptOkRec : Int, clientSet : Set[ActorRef], opIds : Set[Int], n : Int) = {
    this.vaLst = vaLst
    this.naLst = naLst
    this.v = v
    this.acceptOkRec = acceptOkRec
    this.clientSet = clientSet
    this.opIds = opIds
    this.n = n
    
  }
  
  def newCopy(): ProposerSt = {
    var res = new ProposerSt()
    var aux1 = this.vaLst
    var aux2 = this.naLst
    var aux3 = this.v
    var aux4 = this.acceptOkRec
    var aux5 = this.clientSet
    var aux6 = this.opIds
    var aux7 = this.n
    
    res.setContent(aux1, aux2, aux3, aux4, aux5, aux6, aux7)
    return res
  }
}