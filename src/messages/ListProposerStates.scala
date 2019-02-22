package messages

import states.ProposerSt
import collection.mutable.HashMap

case class ListProposerStates (sts: HashMap[String,ProposerSt])