package actors

import akka.actor.Actor
import akka.actor.ActorRef
import messages._
import java.io._
import java.util.Locale

class Statistics(locator: ActorRef) extends Actor {
  
 val paxosType = "Paxos"
 val dynamoType = "Dynamo"
  
  var statsReceived = List[Stats]()
  
  
  def receive = {
    case Stats(opsTime, nOps, writeTimes, readTimes, algType) => {
      statsReceived ::= Stats(opsTime,nOps,writeTimes,readTimes, algType)
      /*Time taken for each write and read*/
      var auxStructWrites = List[List[Double]]()
      var auxStructReads = List[List[Double]]()
      
      val paxos = statsReceived.filter { x => x.algorithmType == paxosType }
      val dynamo = statsReceived.filter { x => x.algorithmType == dynamoType }
      
      println("Paxos clients: "+paxos.size)
      println("Dynamo clients: "+dynamo.size)
      
      if(paxos.size>0){
        for(i <- 0 to paxos.size-1){
          auxStructWrites ::= paxos(i).getWriteTimes()
          auxStructReads ::= paxos(i).getReadTimes()
      	}
        printOpsTimes(new File("writesP.dat"),auxStructWrites)
        printOpsTimes(new File("readsP.dat"), auxStructReads)
      }
      
      if(dynamo.size>0){
        auxStructWrites = List[List[Double]]()
        auxStructReads = List[List[Double]]()
        for(i <- 0 to dynamo.size-1){
          auxStructWrites ::= dynamo(i).getWriteTimes()
          auxStructReads ::= dynamo(i).getReadTimes()
      	}
        printOpsTimes(new File("writesD.dat"),auxStructWrites)
        printOpsTimes(new File("readsD.dat"), auxStructReads)
      }
      println("Files printed")
      println("---------------------------------------------------------------------------------------------------------------------------------")
    }
  }
  
  
  def calcAverageOpsPerSec(): Double = {
    var res = Double.NaN
    res = 0
    for(stat <- statsReceived){
      res += stat.getNOps()/stat.getOpsTime()
    }
    
    res = res/statsReceived.size
    
    return res
  }
  
  def calcVarianceOpsPerSec(): Double = {
    var res = Double.NaN
    res = 0
    if(statsReceived.size > 1){
    	var avg = calcAverageOpsPerSec()
    	for(stat <- statsReceived){
    		res = res + scala.math.pow(((stat.getNOps()/stat.getOpsTime()) - avg), 2) 
    	}

    	res = res/(statsReceived.size - 1)
    }
    return res
  }
  
  def calcStandardDeviationOpsPerSec(): Double = {
    return scala.math.sqrt(calcVarianceOpsPerSec())
  }
  
  def printOpsTimes(f:File, opsTimes: List[List[Double]]) = {
    val p = new PrintWriter(f)
    for(i <- 0 to opsTimes(0).size-1){//All arrays will have the same size
        for(j <- 0 to opsTimes.size-1){
          if(j < opsTimes.size-1)
            p.print("%.7f\t".format(opsTimes(j)(i)).replace(",", "."))
          else
            p.print("%.7f\n".format(opsTimes(j)(i)).replace(",", "."))
        }
    }
    p.close()
  }
  
  locator ! RegisterStatServer(self)
}