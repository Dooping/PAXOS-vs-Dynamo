package states

class AcceptorSt() {
  
  private var np = -1
  
  private var na = -1
  
  private var va = ""
  
  def getNp() = np
  
  def setNp(n: Int) = { np = n }
  
  def getNa() = na
  
  def setNa(n: Int) = { na = n }
  
  def getVa() = va
  
  def setVa(v: String) = { va = v }
  
  private def setContent(np : Int, na : Int, va : String) = {
    this.np = np
    this.na = na
    this.va = va
  }
  
  def newCopy(): AcceptorSt = {
    var res = new AcceptorSt()
    var aux1 = this.np
    var aux2 = this.na
    var aux3 = this.va
    res.setContent(aux1, aux2, aux3)
    return res
  }
}