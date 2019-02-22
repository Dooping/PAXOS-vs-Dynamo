package crdt

import collection.mutable.Set

class CRDT {
  
  var setPairs = Set[(String,Long)]()
  var setRemIds = Set[Long]()
  
  def add(elem: String) = {
    val aux = (elem,System.nanoTime())
    
    setPairs += aux
  }
  
  def join(crdt: CRDT) = {
    setPairs ++= crdt.setPairs
    setRemIds ++= crdt.setRemIds
    setPairs = setPairs.filterNot(x => setRemIds.contains(x._2))
  }
  
  def remove(elem: String) = {
    var elemsToRem = setPairs.filter(e => e._1.equals(elem))
    
    for(e <- elemsToRem){
      setPairs -= e
      setRemIds += e._2
    }
  }
  
  def isElement(elem: String) : Boolean = {
    val e = setPairs.filter(e => e._1.equals(elem))
    if(e.size>0)
      return true
    else
      return false
  }
  
  def list() : List[String] = {
    val unziped = setPairs.unzip
    return unziped._1.toList
  }
}