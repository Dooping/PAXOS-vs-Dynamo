package messages

case class Put(k: String, context: List[(String,Int)], values: List[String])