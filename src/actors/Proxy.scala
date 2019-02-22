package actors

import akka.actor.Actor
import akka.actor.ActorRef
import messages._
import states._
import collection.mutable.HashMap
import scala.util.Random
import crdt._

class Proxy extends Actor{
  
  val N_IDS = 64
  
  val OP_INSERT = "insert"
  val OP_REMOVE = "remove"
  val OP_ISELEMENT = "isElement"
  val OP_LIST = "list"
  
  var locator = self
  
  var kRep = 3
  
  var serversPaxos = List[ActorRef]()
  var serversDynamo = List[ActorRef]()
  
  val paxosType = "Paxos"
  val dynamoType = "Dynamo"
  
  var nodeHashTablePaxos = HashMap[Int,Set[ActorRef]]()
  var nodeHashTableDynamo = HashMap[Int,Set[ActorRef]]()
  
  //Will represent an Id to associate with a given operation
  var opSeqN = 0
  
  var proxyStates = HashMap[String,ProxySt]()
  
  //Map<opSeqN,(client,leader,operation,key)>
  var paxosProxyStates = HashMap[Int,(ActorRef,ActorRef,String,String)]()
  
  //List of the opIds when a server management must occur
  var opIdManageServers = List[Int](100)
  
  def receive = {
    case Register(servers, this.paxosType) => {
        if(locator.equals(self))
          locator = sender
    	  serversPaxos = servers
    	  nodeHashTablePaxos = calcNodeHashTable(serversPaxos)
    	  refreshPaxosStates()
    }
    case Register(servers, this.dynamoType) => {
      if(locator.equals(self))
          locator = sender
        serversDynamo = servers
        nodeHashTableDynamo = calcNodeHashTable(serversDynamo)
        refreshDynamoStates()
    }
    case LocateResult(servers, this.paxosType) => {
    	  serversPaxos = servers
    	  nodeHashTablePaxos = calcNodeHashTable(serversPaxos)
    }
    case LocateResult(servers, this.dynamoType) => {
        serversDynamo = servers
        nodeHashTableDynamo = calcNodeHashTable(serversDynamo)
    }
    case ChangeKRep(nRep,this.paxosType) => {
      kRep = nRep
      nodeHashTablePaxos = calcNodeHashTable(serversPaxos)
    }
    case ChangeKRep(nRep,this.dynamoType) => {
      kRep = nRep
      nodeHashTableDynamo = calcNodeHashTable(serversDynamo)
    }
    case Write(k,op,this.paxosType) => writePaxos(k,op,sender)
    case Write(k,op,this.dynamoType) => writeDynamo(k,op,dynamoType)
    case Read(k,op,this.paxosType) => readPaxos(k,op,sender)
    case Read(k,op,this.dynamoType) =>  readDynamo(k,op,dynamoType)
    case IsElementResult(k,res,opId) => {
      if(paxosProxyStates.contains(opId)){
        //We can remove since an answer to the client will be sent now
        val auxSt = paxosProxyStates.remove(opId).get
        
        auxSt._1 ! IsElementResult(k,res,opId)
      }
    }
    case ListResult(k,res,opId) => {
      if(paxosProxyStates.contains(opId)){
        //We can remove since an answer to the client will be sent now
        val auxSt = paxosProxyStates.remove(opId).get
        
        auxSt._1 ! ListResult(k,res,opId)
      }
    }
    case Decided(k,opId) => {
      if(paxosProxyStates.contains(opId)){
        //We can remove since an answer to the client will be sent now
        val auxSt = paxosProxyStates.remove(opId).get
        
        auxSt._1 ! Decided(k,opId)
      }
    }
    case GetResult(k,context,values,opId) => {
      if(proxyStates.contains(k)){
        
        proxyStates(k).addGetResultRec(opId, context, values, sender)
        
        //Which means all of the replicas that were sent the Get message already answered with GetResult for this operation
        if(proxyStates(k).getNumGetResults(opId) == proxyStates(k).getQuorumSize(opId)){
          
          val opName = proxyStates(k).getOperation(opId)
          
          val res = combine(k,opId)
          
          if(opName.contains(OP_INSERT) || opName.contains(OP_REMOVE)){
            sendPut(k,opId,res._1, res._2)
          }else if(opName.contains(OP_ISELEMENT) || opName.contains(OP_LIST)){
            sendClientAnswer(k,opId,res._2)
          }
          
        }
      }
      
    }
  }
  
  def writePaxos(k:String, op:String, sender: ActorRef) = {
    notifyServerManagement()

    val leader = getLeader(k)
    leader ! Write(k,op,this.opSeqN+"")
    
    paxosProxyStates += (this.opSeqN -> (sender,leader,op,k))
    opSeqN += 1 
    
  }
  
  def readPaxos(k:String, op:String, sender: ActorRef) = {
    notifyServerManagement()
    
    val leader = getLeader(k)
    leader ! Read(k,op,this.opSeqN+"")
    
    paxosProxyStates += (this.opSeqN -> (sender,leader,op,k))
    opSeqN += 1 
  }
  
  def writeDynamo(k:String, op:String, algType: String) = {
    notifyServerManagement()
    
    //Read from quorum
    var quorum = getQuorum(k,algType)
    for(s <- quorum)
    {
      s ! Get(k, this.opSeqN)
    }
    
    if(!proxyStates.contains(k))
      proxyStates += (k -> new ProxySt())
    
    
    proxyStates(k).initOp(opSeqN, op, sender, quorum)
    
    opSeqN += 1  
  }
  
  def readDynamo(k:String, op:String, algType: String) = {
    notifyServerManagement()
    
    //Read from quorum
    var quorum = getQuorum(k,algType)
    for(s <- quorum)
    {
      s ! Get(k, this.opSeqN)
    }
    
    if(!proxyStates.contains(k))
      proxyStates += (k -> new ProxySt())
    
    
    proxyStates(k).initOp(opSeqN, op, sender, quorum)
    
    opSeqN += 1
  }
  
  def notifyServerManagement() = {
    if(this.opIdManageServers.size > 0){
      if(!locator.equals(self) && this.opSeqN == this.opIdManageServers(0)){
        
    	  this.opIdManageServers = this.opIdManageServers.drop(1)
        locator ! ManageServers(serversPaxos,serversDynamo)
        
      }
      
    }
    
  }
  
  def calcNodeHashTable(servers: List[ActorRef]): HashMap[Int,Set[ActorRef]] = {
    var auxLst = List[(Int,ActorRef)]()
    
    var nodeHashTable = HashMap[Int,Set[ActorRef]]()
    
    for(s <- servers)
      auxLst ::= ((s.hashCode()%N_IDS+N_IDS)%N_IDS,s)
    
    auxLst=auxLst.sortWith(_._1<_._1)
    
    for(i <- 0 to N_IDS-1){
      nodeHashTable += (i -> Set[ActorRef]())
    }
    
    var k = 0
    for(i <- 0 to auxLst.size-1){
      for(j <- k to auxLst(i)._1)
        for(l <- 0 to kRep-1)
          nodeHashTable(j%N_IDS) +=auxLst((i+l)%auxLst.size)._2
      
      k = auxLst(i)._1 + 1
    }
    for(i <- auxLst(auxLst.size-1)._1+1 to N_IDS-1)
      for(l <- 0 to kRep-1)
          nodeHashTable(i) +=auxLst((l)%auxLst.size)._2 
          
    return nodeHashTable
  }
  
  def getQuorum(key: String, algType: String): List[ActorRef] = {
    var res = List[ActorRef]()
    
    var auxElems = Seq[ActorRef]()
    var nElems = 0
    if(algType.equals(paxosType)){ 
    	  return (nodeHashTablePaxos(calcIndex(key))).toList
    }
    else if(algType.equals(dynamoType)){
        auxElems = nodeHashTableDynamo(calcIndex(key)).toSeq
        nElems = (math.floor(kRep/2) + 1).toInt
    }
    
    val r = new Random(System.nanoTime())
    
    val idx = r.nextInt(auxElems.size)
    for(i <- 0 to nElems-1)
      res ::= auxElems((idx+i)%auxElems.size)
    
    return res
  }
  
  def calcIndex(key: String) = (key.hashCode()%N_IDS+N_IDS)%N_IDS
  
  //Return context,values
  def combine(k: String, opId: Int) : (List[(String,Int)], List[String]) = {
    if(proxyStates.contains(k)){      
    	return proxyStates(k).combineGetResults(opId);
    }
    
    return null
  }
  
  def sendPut(k: String, opId: Int, context: List[(String,Int)], values: List[String]) = {
    if(proxyStates.contains(k)){
      var auxCrdt = new CRDT()
      
      for(v <- values){
        auxCrdt.add(v)
      }
      
      val op = proxyStates(k).getOperation(opId)
      
      //Execute new operation over local state
      var value = ""
      if(op.contains(OP_INSERT))
      {
        value = op.stripPrefix(OP_INSERT+"(")
        value = value.stripSuffix(")")
        auxCrdt.add(value)
      }else if(op.contains(OP_REMOVE)){
        value = op.stripPrefix(OP_REMOVE+"(")
        value = value.stripSuffix(")")
        auxCrdt.remove(value)
      }
      
      //Send Put message to the quorum
      var quorum = proxyStates(k).getQuorum(opId)
      for(s <- quorum){
    	  s ! Put(k,context,auxCrdt.list())
      }
      
      proxyStates(k).getClient(opId) ! Decided(k,opId)
      proxyStates(k).removeOp(opId)
    }
  }
  
  def sendClientAnswer (k: String, opId: Int, values: List[String]) = {
    if(proxyStates.contains(k)){
      var auxCrdt = new CRDT()
      
      for(v <- values){
        auxCrdt.add(v)
      }
      
      val op = proxyStates(k).getOperation(opId)
      
      //Execute operation
      var value = ""
      if(op.contains(OP_ISELEMENT))
      {
        value = op.stripPrefix(OP_ISELEMENT+"(")
      	value = value.stripSuffix(")")
      	
        proxyStates(k).getClient(opId) ! IsElementResult(k,auxCrdt.isElement(value),opId)
          
      }else if(op.contains(OP_LIST)){
        proxyStates(k).getClient(opId) ! ListResult(k,auxCrdt.list(),opId)
        
      }
      
      proxyStates(k).removeOp(opId)
    }
  }
  
  def refreshPaxosStates() = {
    var setToRem = Set[Int]()
    
    for(st <- paxosProxyStates){
        
        //Add opId to remove since we are finally sending an answer to the client
        var aux = st._2
        setToRem += st._1
        if(aux._3.contains(OP_INSERT) || aux._3.contains(OP_REMOVE))
          aux._1 ! Decided("-1",st._1)
        else if(aux._3.contains(OP_ISELEMENT))
          aux._1 ! IsElementResult("-1",false,st._1)
        else if(aux._3.contains(OP_LIST))
          aux._1 ! List("-1",List[String](),st._1)

        self ! OperationError()
      
    }
    
    paxosProxyStates = paxosProxyStates.filter(elem => !setToRem.contains(elem._1))
  }
  
  def refreshDynamoStates() = {
    var newSet = Set[ActorRef]()
    var elemsToSendGet = Set[ActorRef]()
    for(st <- proxyStates){
      newSet = nodeHashTableDynamo(calcIndex(st._1))
      
      for(opId <- st._2.getAllOpsIds()){
        elemsToSendGet = st._2.refreshStateOp(newSet, opId)
        
        for(server <- elemsToSendGet)
          server ! Get(st._1, opId)
      }
      
    }
    
    
  }
  
  def getLeader(k: String):ActorRef = {
    val set = nodeHashTablePaxos((k.hashCode()%N_IDS+N_IDS)%N_IDS)
    return set.toList(0)
  }
  
   
}