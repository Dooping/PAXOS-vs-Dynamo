package messages

case class GetResult(k: String, context: List[(String,Int)], values: List[String], opId: Int)