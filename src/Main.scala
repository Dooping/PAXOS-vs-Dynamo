
import akka.actor.ActorSystem
import akka.actor.Props
import actors._
import messages._

object Main {
  
  def main(args: Array[String]): Unit = {
    
    val actorSystem = ActorSystem("akka4scala");
  
    val root = actorSystem.actorOf(Props[Root]);
    
    root ! Create()
  }
  
}