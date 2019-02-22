package actors

import akka.actor.Actor
import akka.actor.ActorRef
import messages._
import states._
import collection.mutable.HashMap
import scala.util.Random

class Server(algorithmType: String) extends Actor {
  
  val N_IDS = 64
  
  var kRep = 3
  
  val PROPOSER = "proposer"
  val ACCEPTOR = "acceptor"
  val LEARNER = "learner"
  
  val OP_INSERT = "insert"
  val OP_REMOVE = "remove"
  val OP_ISELEMENT = "isElement"
  val OP_LIST = "list"
  
  var serversPaxos = List[ActorRef]()
  var serversDynamo = List[ActorRef]()
  
  val paxosType = "Paxos"
  val dynamoType = "Dynamo"
  
  var nodeHashTablePaxos = HashMap[Int,Set[ActorRef]]()
  var nodeHashTableDynamo = HashMap[Int,Set[ActorRef]]()

  var nSeq = -1
  var random = new Random(System.nanoTime())
  
  var proposerStates = HashMap[String,ProposerSt]()
  var acceptorStates = HashMap[String,AcceptorSt]()
  var learnerStates = HashMap[String,LearnerSt]()
  
  var replicaStates = HashMap[String,ReplicaSt]()
  
  def receive = {
    case Register(servers, this.paxosType) =>{
    	  serversPaxos = servers
    	  nodeHashTablePaxos = calcNodeHashTable(serversPaxos)
    	  refreshStatesPaxos()
    }
    case Register(servers, this.dynamoType) =>{
        serversDynamo = servers
        nodeHashTableDynamo = calcNodeHashTable(serversDynamo)
        refreshStatesDynamo()
    }
    case ChangeKRep(nRep,this.paxosType) => {
      kRep = nRep
      calcNodeHashTable(serversPaxos)
    }
    case ChangeKRep(nRep,this.dynamoType) => {
      kRep = nRep
      calcNodeHashTable(serversDynamo)
    }
    case Write(k,op,opId) => {
      //This is for paxos
      nSeq += 1 + random.nextInt(10);
      
      if(!proposerStates.contains(k))
      {	
        var auxProp = new ProposerSt()
    		auxProp.setN(nSeq)
      	auxProp.setV(op)
      	proposerStates+=(k -> auxProp)
      }else{
        proposerStates(k).setN(nSeq)
        proposerStates(k).setV(op)
      }
    	
    	proposerStates(k).addClient(sender,opId.toInt)
      	
    	if(isLeader(k)){
    	  //Proposing value v to all servers
    		for(serv <- getReplicas(k)){
    			serv ! Accept(nSeq,op,k,opId.toInt)
    		}
    	}
    }
    case Read(k,op,opId) => {
      //This is for paxos
      if(op.contains(OP_ISELEMENT))
    	{
    		if(learnerStates.contains(k))
    		{  
    			val auxList = learnerStates(k).getDecision()
      		var value = ""
      		var res = false
      		value = op.stripPrefix(OP_ISELEMENT+"(")
      		value = value.stripSuffix(")")
  
      		for(s <- auxList)
      		{
      			if(s.equals(value))
      				res = true
      		}
    			sender ! IsElementResult(k,res,opId.toInt)
    		}else
    			sender ! IsElementResult(k,false,opId.toInt)

    	}else if(op.contains(OP_LIST)){
      		if(learnerStates.contains(k))
      		{ 
      			val auxList = learnerStates(k).getDecision()
      					sender ! ListResult(k,auxList,opId.toInt)
      		}else
      			sender ! ListResult(k,List[String](),opId.toInt)
    	}
    }
    case Prepare(n,k) => {//First message sent by a proposer and received by an acceptor
      if(!acceptorStates.contains(k)){
        acceptorStates+=(k -> new AcceptorSt())
      }
      
      if(acceptorStates(k).getNp() < n){
        acceptorStates(k).setNp(n)
        sender ! PrepareOk(acceptorStates(k).getNa(),acceptorStates(k).getVa(),k)
      }else{
        
        if(learnerStates.contains(k)){
          var dec = learnerStates(k).getDecision()
          
          if(!dec.equals("")){//Consensus has been reached
            sender ! AcceptOk(learnerStates(k).getNa(),None,k, -1)
          }
          
        }
        
      }
      
    }
    case PrepareOk(n,v,k) => {//Message sent as a reply to an initial Prepare, from an acceptor to a proposer
      if(proposerStates.contains(k)){
        proposerStates(k).addVa(n, v)

        if((math.floor(kRep/2) + 1) <= proposerStates(k).size()){//Check if a majority of acceptors already sent a prepareOk
          var vSend = ""
          
          if(proposerStates(k).getHighestVa().equals(""))
        	  vSend = proposerStates(k).getV()
          else{
            vSend = proposerStates(k).getHighestVa()
            proposerStates(k).setV(v)//update proposer value
          }
          
          for(serv <- getReplicas(k)){
        	  serv ! Accept(proposerStates(k).getN(),vSend,k, -1)
          }
        }
      }
    }
    case Accept(n,v,k,opId) => {//Message sent from the proposer to the acceptors in case a majority of acceptors compromised to receive the value
      
      if(!acceptorStates.contains(k)){
        acceptorStates+=(k -> new AcceptorSt())
      }
      
      if(n >= acceptorStates(k).getNp()){
        acceptorStates(k).setNa(n)
        acceptorStates(k).setVa(v)
        
        //sender ! AcceptOk(n,None,k)//Send to proposer
        
        for(serv <- getReplicas(k)){//Send to all learners
        	  serv ! AcceptOk(n,Option(v),k,opId)
        }
      }
      
    }
    case AcceptOk(n,v,k,opId) => {//Message sent from the acceptors to the remaining roles notifying that they accepted the proposed value
      
      if(v != None){

        if(!learnerStates.contains(k)){
          learnerStates+=(k -> new LearnerSt())
        }
        
        if(n >= learnerStates(k).getNa()){
          
          if(n > learnerStates(k).getNa()){
        	  learnerStates(k).setNa(n)
        	  learnerStates(k).addOp(v.get)
          }
          
          learnerStates(k).addActor(v.get,sender)
          
          if((math.floor(kRep/2) + 1) <= learnerStates(k).size(v.get))//Check if aset is a quorum
          { 
            learnerStates(k).setDecision(v.get)
            
        	  if(proposerStates.contains(k)){
        		  for(cl <- proposerStates(k).getClientSet()){
        		    for(id <- proposerStates(k).getOpIds())
        		    { 
        		      cl ! Decided(k,id) 
        		    }
        		  }

        		  proposerStates(k).remClients()
        	  }
            
          }

        }
        
      }else{
        
    	  if(proposerStates.contains(k)){
    		  proposerStates(k).incAcceptOkRec()
    		  
    		  if((math.floor(kRep/2) + 1) <= proposerStates(k).getAcceptOkRec()){

    			  for(cl <- proposerStates(k).getClientSet()){
    				  cl ! Decided(k,opId)
    			  }

    			  proposerStates(k).remClients()
    		  }
    	  }
        
      }
      
    }
    case Get(k, opId) => {
      if(!replicaStates.contains(k)){
        var auxRepSt = new ReplicaSt()
        auxRepSt.initContext(nodeHashTableDynamo(calcIndex(k)))
        replicaStates += (k -> auxRepSt)
      }

      val auxSt = replicaStates(k)
      
      sender ! GetResult(k, auxSt.getContext(), auxSt.getValues(), opId)

    }
    case Put(k,context,values) => {
      if(!replicaStates.contains(k)){
        var auxRepSt = new ReplicaSt()
        auxRepSt.initContext(nodeHashTableDynamo(calcIndex(k)))
        replicaStates += (k -> auxRepSt)
      }
      
      replicaStates(k).updateState(values, context, self.path.name)
    }
    case GetStates(i , serverType) => {
    	if(paxosType.equals(serverType)){
    	  var auxIndex = -1
    	  //Proposer States
    		var auxProposerPaxos = HashMap[String,ProposerSt]()
    		for(st <- proposerStates){
    		  auxIndex = calcIndex(st._1)
    		  if(auxIndex == i)
    			  auxProposerPaxos += new Tuple2(st._1,st._2.newCopy())
    		}
    		
    		//Acceptor States
    		var auxAcceptorPaxos = HashMap[String,AcceptorSt]()
    		for(st <- acceptorStates){
    		  auxIndex = calcIndex(st._1)
    		  if(auxIndex == i)
    			  auxAcceptorPaxos += new Tuple2(st._1,st._2.newCopy())
    		}
    		
    		//Learner States
    		var auxLearnerPaxos = HashMap[String,LearnerSt]()
    		for(st <- learnerStates){
    		  auxIndex = calcIndex(st._1)
    		  if(auxIndex == i)
    			  auxLearnerPaxos += new Tuple2(st._1,st._2.newCopy())
    		}
    		
    		sender ! ListProposerStates(auxProposerPaxos)
    		sender ! ListAcceptorStates(auxAcceptorPaxos)
    		sender ! ListLearnerStates(auxLearnerPaxos)
    	}else if(dynamoType.equals(serverType)){
    		var auxDynamo = HashMap[String,ReplicaSt]()
    		for(st <- replicaStates){
    		  var auxIndex = calcIndex(st._1)
    		  if(auxIndex == i)
    			  auxDynamo += new Tuple2(st._1,st._2.newCopy())
    		}
    		sender ! ListStatesDynamo(auxDynamo)
    	}
    }
    case ListProposerStates(sts) => {
    	for(st <- sts){
    		if(!proposerStates.contains(st._1))
    		  proposerStates += st
    	}
    }
    case ListAcceptorStates(sts) => {
    	for(st <- sts){
    		if(!acceptorStates.contains(st._1))
    		  acceptorStates += st
    	}
    }
    case ListLearnerStates(sts) => {
    	for(st <- sts){
    		if(!learnerStates.contains(st._1))
    		  learnerStates += st
    	}
    }
    case ListStatesDynamo(sts) => {
    	for(st <- sts){
    		if(!replicaStates.contains(st._1))
    			replicaStates += st
    	}
    }
  }
  
  def isLeader(k: String):Boolean = {
    val set = nodeHashTablePaxos((k.hashCode()%N_IDS+N_IDS)%N_IDS)
    val index = set.toList.indexOf(self)
    return index == 0
  }
  
  def getLeader(k: String):ActorRef = {
    val set = nodeHashTablePaxos((k.hashCode()%N_IDS+N_IDS)%N_IDS)
    return set.toList(0)
  }
    
  def getReplicas(k: String):Set[ActorRef] = {
    return nodeHashTablePaxos((k.hashCode()%N_IDS+N_IDS)%N_IDS)
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
    	  return (nodeHashTablePaxos((key.hashCode()%N_IDS+N_IDS)%N_IDS)).toList
    }
    else if(algType.equals(dynamoType)){
        auxElems = nodeHashTableDynamo((key.hashCode()%N_IDS+N_IDS)%N_IDS).toSeq
        nElems = (math.floor(kRep/2) + 1).toInt
    }
    
    val r = new Random(System.nanoTime())
    
    val idx = r.nextInt(auxElems.size)
    for(i <- 0 to nElems-1)
      res ::= auxElems((idx+i)%auxElems.size)
    
    return res
  }
  
  def calcIndex(key: String) = (key.hashCode()%N_IDS+N_IDS)%N_IDS

  def refreshStatesPaxos() = {
		  refreshStatesPaxosAdd()
		  refreshStatesPaxosRem()
  }

  def refreshStatesPaxosAdd() = {
		  for(entry <- nodeHashTablePaxos){
			  var auxIndex = entry._1
				var auxSet = entry._2

				//If this server is in auxSet
				if(auxSet.contains(this.self)){
				  auxSet -= this.self
					for(s <- auxSet)
					  s ! GetStates(auxIndex, paxosType)
				}
		  }
  }

  def refreshStatesPaxosRem() = {

		  for(st <- learnerStates){
			  var auxIndex = calcIndex(st._1)
				var auxSet = nodeHashTablePaxos(auxIndex)
				if(!auxSet.contains(this.self)){
				   learnerStates -= st._1
				}
		  }
  }

  def refreshStatesDynamo() = {
    //println("Refresh states dynamo")
		  refreshStatesDynamoAdd()
		  refreshStatesDynamoRem()
  }

  def refreshStatesDynamoAdd() = {
		  for(entry <- nodeHashTableDynamo){
			  var auxIndex = entry._1
				var auxSet = entry._2
				
				//If this server is in auxSet
				if(auxSet.contains(this.self)){
				  auxSet -= this.self
					for(s <- auxSet)
					  s ! GetStates(auxIndex, dynamoType)
				}
		  }
  }

  def refreshStatesDynamoRem() = {
		  for(st <- replicaStates){
			  var auxIndex = calcIndex(st._1)
				var auxSet = nodeHashTableDynamo(auxIndex)
				if(!auxSet.contains(this.self)){
				  replicaStates -= st._1
				}
		  }
  }
	
}