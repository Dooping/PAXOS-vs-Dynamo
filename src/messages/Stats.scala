package messages

case class Stats(opsTime: Double, nOps: Int, writeTimes: List[Double], readTimes: List[Double], algorithmType: String) {
  
  def getOpsTime() = opsTime
  
  def getNOps() = nOps
  
  def getWriteTimes() = writeTimes
  
  def getReadTimes() = readTimes
}