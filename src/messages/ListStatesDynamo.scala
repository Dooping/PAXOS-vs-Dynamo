package messages

import states.ReplicaSt
import collection.mutable.HashMap

case class ListStatesDynamo (sts: HashMap[String,ReplicaSt])