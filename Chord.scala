import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import util.control.Breaks._
import scala.concurrent.duration._

sealed trait message
case object Initialize extends message
case object Start extends message
case class add(valueAdd: Integer) extends message
case class Join(newNode: ActorRef) extends message
case class findSuccessor(fid: Integer) extends message
case class findPredecessor(fid: Integer) extends message
case class res(nod: ActorRef) extends message
case object YourSuccessor extends message
case class resId(nodId: Integer) extends message
case object YourId extends message
case class findClosestPrecFinger(fid: Integer) extends message
case class both(nod: ActorRef, nodId: Integer) extends message
case class all(nod: ActorRef, nodId: Integer, succ: ActorRef, succId: Integer) extends message
case object YourPredecessor extends message
case class updatePred(pred: ActorRef, predId: Integer) extends message
case class updateFingerTable(nod: ActorRef, nodId: Integer, i: Integer) extends message
case object Print extends message
case class InitFingerTable(refNode: ActorRef) extends message
case object UpdateOthers extends message
case object StartSearch extends message
case class Search(key:Integer) extends message
case object DoSearch extends message
case class Found(n:Integer) extends message
case class Find(key:Integer) extends message


/*
 * Main block
 */
object Chord extends App {
  if (args.length != 2) {
    println("Rerun the protocol with arguments number of nodes and requests")
  } else {
    val system = ActorSystem("Chord")
    val numOfNodes:Integer = args(0).toInt
    val numOfRequests:Integer = args(1).toInt
    val idLimiter = math.ceil(math.log(args(0).toDouble) / math.log(2)) + 1
    val master = system.actorOf(Props(new master(numOfNodes,numOfRequests, idLimiter.toInt)), name = "master")
    master ! Start
  }

}


/*
 * Finger
 */
class finger {
  var start: Integer = null
  var end: Integer = null
  var successor: ActorRef = null
  var successorId: Integer = 0
}

/*
 * Node
 */
class Node(value: String, hash: Integer, bitLimiter: Integer) extends Actor {
  var name: String = value
  var id: Integer = hash
  var successor: ActorRef = null
  var successorId: Integer = 0
  var predecessor: ActorRef = null
  var predecessorId: Integer = 0
  var fingerTable: Array[finger] = new Array[finger](bitLimiter)
  var cross: Integer = math.pow(2, bitLimiter.toDouble).toInt
  var keys:ArrayBuffer[Integer] = new ArrayBuffer[Integer]

  /*
   * Initializing finger table
   */
  def InitFingers() = {
    fingerTable(0) = new finger
    fingerTable(0).start = (id + 1) % cross
    fingerTable(0).end = (id + 2) % cross
    for (i <- 1 until fingerTable.length) {
      fingerTable(i) = new finger
      fingerTable(i).start = fingerTable(i - 1).end
      fingerTable(i).end = (id + math.pow(2, i + 1).toInt) % cross
    }
  }

  def initFingerTable(refNode: ActorRef) {

    implicit var timeout = Timeout(5000)
    var future: Future[both] = ask(refNode, findSuccessor(fingerTable(0).start)).mapTo[both]
    future.onSuccess {
      case both(nod: ActorRef, nodId: Integer) =>

        fingerTable(0).successor = nod
        fingerTable(0).successorId = nodId
        successor = fingerTable(0).successor
        successorId = fingerTable(0).successorId
        timeout = Timeout(5000)
        var future2: Future[both] = ask(successor, YourPredecessor).mapTo[both]
        future2.onSuccess {
          case both(nod: ActorRef, nodId: Integer) =>
            predecessor = nod
            predecessorId = nodId
            timeout = Timeout(5000)
            var future3: Future[String] = ask(successor, updatePred(self, id)).mapTo[String]
            future3.onSuccess {
              case "Update_Done" =>
                for (i <- 0 until fingerTable.length - 1) {
                  if (((id <= fingerTable(i + 1).start) && (fingerTable(i + 1).start < fingerTable(i).successorId)) || (id==fingerTable(i).successorId) || ((id > fingerTable(i).successorId) && (((id <= fingerTable(i + 1).start) && (fingerTable(i + 1).start < (fingerTable(i).successorId + cross))) || ((id <= (fingerTable(i + 1).start + cross)) && ((fingerTable(i + 1).start + cross) < (fingerTable(i).successorId + cross)))))) {
                    fingerTable(i + 1).successor = fingerTable(i).successor
                    fingerTable(i + 1).successorId = fingerTable(i).successorId
                  } else {
                    timeout = Timeout(5000)
                    var future4: Future[both] = ask(refNode, findSuccessor(fingerTable(i + 1).start)).mapTo[both]
                    future4.onSuccess {
                      case both(nod1: ActorRef, nod1Id: Integer) =>
                        fingerTable(i + 1).successor = nod1
                        fingerTable(i + 1).successorId = nod1Id

                    }
                    Thread.sleep(200)
                  }
                }
            }
        }

    }
    Thread.sleep(400)

  }

  def fndClosestPrecFinger(fid: Integer):Integer = {
      var i:Integer =0
      for (j <- 0 until fingerTable.length) {
        i = fingerTable.length -1 -j
        
        if ((((id < fingerTable(i).successorId) && (fingerTable(i).successorId < fid)) || ((id == fid) && (fingerTable(i).successorId != id))|| ((id > fid) && (((id < fingerTable(i).successorId) && (fingerTable(i).successorId < (fid + cross))) || ((id < (fingerTable(i).successorId + cross)) && ((fingerTable(i).successorId + cross) < (fid + cross))))))) {
          var finNode: ActorRef = fingerTable(i).successor
          var finNodeId: Integer = fingerTable(i).successorId
          var finNodeSuc: ActorRef = null
          var finNodeSucId: Integer = 0
          return i
        }
      }
      return null
  }

  def receive = {
    case Initialize =>
      InitFingers()
      successor = self
      successorId = id
      predecessor = self
      predecessorId = id
      for (i <- 0 until fingerTable.length) {
        fingerTable(i).successor = self
        fingerTable(i).successorId = id
      }
      sender ! "JoinDone"

    case updatePred(pred: ActorRef, predId: Integer) => {
      predecessor = pred
      predecessorId = predId
      sender ! "Update_Done"

    }

    case UpdateOthers =>
      var mainSender: ActorRef = sender
      var counter: Integer = 0
      var reqId: Integer = 0
      for (i <- 0 until fingerTable.length) {
        reqId = id - math.pow(2, i).toInt
        if (reqId < 0) {
          reqId = reqId + cross
        }
        implicit var timeout = Timeout(5000)
        var future: Future[both] = ask(self, findPredecessor(reqId)).mapTo[both]
        var pred: ActorRef = null
        var predId: Integer = 0
        future.onSuccess {
          case both(nod: ActorRef, nodId: Integer) =>
            pred = nod
            predId = nodId
            implicit var timeout = Timeout(5000)
            var future2: Future[String] = ask(pred, updateFingerTable(self, id, i)).mapTo[String]
            future2.onSuccess {
              case "Update_Done" =>
                counter = counter + 1
                if (counter == fingerTable.length) {
                  mainSender ! "Update_Done"
                }

            }
        }
        Thread.sleep(400)
      }

    case updateFingerTable(nod: ActorRef, nodId: Integer, i: Integer) => {
      var mainSender: ActorRef = sender
      if (((id < nodId) && (nodId < fingerTable(i).successorId)) || ((nodId != id) && (id == fingerTable(i).successorId))|| ((id > fingerTable(i).successorId) && (((id < nodId) && (nodId < (fingerTable(i).successorId + cross))) || ((id < (nodId + cross)) && ((nodId + cross) < (fingerTable(i).successorId + cross)))))) {
        fingerTable(i).successor = nod
        fingerTable(i).successorId = nodId
        if (i == 0) {
          successor = nod
          successorId = nodId
        }
        if(nodId != predecessorId){
          implicit var timeout = Timeout(5000)
          var future2: Future[String] = ask(predecessor, updateFingerTable(nod, nodId, i)).mapTo[String]
          var pred: ActorRef = null
          future2.onSuccess {
            case "Update_Done" =>
              mainSender ! "Update_Done"

          }
        }
        else{
          mainSender ! "Update_Done"
        }
        
      }
      else{
        mainSender ! "Update_Done"
      }

    }

    case Join(referenceNode: ActorRef) =>
      var mainSender: ActorRef = sender
      var boss: ActorRef = self
      InitFingers()
      implicit var timeout = Timeout(5000)
      var future: Future[String] = ask(self, InitFingerTable(referenceNode)).mapTo[String]
      future.onSuccess {
        case "init_finger_done" =>
          implicit var timeout = Timeout(8000)
          var future2: Future[String] = ask(self, UpdateOthers).mapTo[String]
          future2.onSuccess {
            case "Update_Done" =>
              mainSender ! "JoinDone"
          }
          Thread.sleep(100)
      }

    case InitFingerTable(refNode: ActorRef) =>
      var mainSender: ActorRef = sender
      implicit var timeout = Timeout(5000)
      var future: Future[both] = ask(refNode, findSuccessor(fingerTable(0).start)).mapTo[both]
      future.onSuccess {
        case both(nod: ActorRef, nodId: Integer) =>
          fingerTable(0).successor = nod
          fingerTable(0).successorId = nodId
          successor = fingerTable(0).successor
          successorId = fingerTable(0).successorId
          timeout = Timeout(5000)
          var future2: Future[both] = ask(successor, YourPredecessor).mapTo[both]
          future2.onSuccess {
            case both(nod: ActorRef, nodId: Integer) =>
              predecessor = nod
              predecessorId = nodId
              timeout = Timeout(5000)
              var future3: Future[String] = ask(successor, updatePred(self, id)).mapTo[String]
              future3.onSuccess {
                case "Update_Done" =>
                  for (i <- 0 until fingerTable.length - 1) {
                    if (((id <= fingerTable(i + 1).start) && (fingerTable(i + 1).start < fingerTable(i).successorId)) || (id == fingerTable(i).successorId) || ((id > fingerTable(i).successorId) && (((id <= fingerTable(i + 1).start) && (fingerTable(i + 1).start < (fingerTable(i).successorId + cross))) || ((id <= (fingerTable(i + 1).start + cross)) && ((fingerTable(i + 1).start + cross) < (fingerTable(i).successorId + cross)))))) {
                      fingerTable(i + 1).successor = fingerTable(i).successor
                      fingerTable(i + 1).successorId = fingerTable(i).successorId
                    } else {
                      timeout = Timeout(5000)
                      var future4: Future[both] = ask(refNode, findSuccessor(fingerTable(i + 1).start)).mapTo[both]
                      future4.onSuccess {
                        case both(nod1: ActorRef, nod1Id: Integer) =>
                          fingerTable(i + 1).successor = nod1
                          fingerTable(i + 1).successorId = nod1Id
                      }
                      Thread.sleep(200)
                    }
                  }

              }
          }

      }
      Thread.sleep(400)
      mainSender ! "init_finger_done"

    case Print =>
      var printStr = "The finger table for the node with name " + name + " and id " + id + " is : "
      for (i <- 0 until fingerTable.length) {
        printStr += "(" + fingerTable(i).start + "," + fingerTable(i).end + "," + fingerTable(i).successorId + ")"
      }
      println(printStr)

    /*
     * returns successor of an id  
     */
    case findSuccessor(fid: Integer) => {
      val mainSender: ActorRef = sender
      var suc: ActorRef = null
      var sucId: Integer = 0
      if (id == fid) {
        mainSender ! both(successor, successorId)
      } else {
        implicit var timeout = Timeout(5000)
        var future: Future[both] = ask(self, findPredecessor(fid)).mapTo[both]
        var pred: ActorRef = null
        future.onSuccess {
          case both(nod: ActorRef, nodId: Integer) =>
            pred = nod
            timeout = Timeout(5000)
            var future2: Future[both] = ask(pred, YourSuccessor).mapTo[both]
            future2.onSuccess {
              case both(nod: ActorRef, nodId: Integer) =>

                suc = nod
                sucId = nodId
                mainSender ! both(suc, sucId)

            }

        }

      }
    }

    /*
     * returns predecessor of an id
     */
    case findPredecessor(fid: Integer) => {
      var mainSender: ActorRef = sender
      var node: ActorRef = self
      var suc: ActorRef = successor
      var nodeId: Integer = id
      var sucId: Integer = successorId
      var brk:Boolean = true
      
        while ((brk) && (!(((nodeId < fid) && (fid <= sucId)) || (nodeId == sucId) || ((nodeId > sucId) && (((nodeId < fid) && (fid <= (sucId + cross))) || ((nodeId < (fid + cross)) && ((fid + cross) <= (sucId + cross)))))))) {
          implicit var timeout = Timeout(5000)
          if(nodeId == id){
            var fingerNumber:Integer = fndClosestPrecFinger(fid)
            if(fingerNumber != null){
              node = fingerTable(fingerNumber).successor
              nodeId = fingerTable(fingerNumber).successorId
            }
            else{
              brk = false
            }
            
          }
          else{
            var future2: Future[both] = ask(node, findClosestPrecFinger(fid)).mapTo[both]
            future2.onSuccess {
              case both(nod: ActorRef, nodId: Integer) =>
                if (nodeId == nodId) {
                  brk =false
                 }
                else{
                  node = nod
                  nodeId = nodId
                }
                  
            }
            Thread.sleep(50)
          }
          if(brk){
            var future: Future[both] = ask(node, YourSuccessor).mapTo[both]
            future.onSuccess {
              case both(nod: ActorRef, nodId: Integer) =>
                suc = nod
                sucId = nodId
                
            }
            Thread.sleep(200)
          }
        }
      
      mainSender ! both(node, nodeId)
    }
    
    case Find(key:Integer) =>
      //Start the search

    case findClosestPrecFinger(fid: Integer) => {
      var mainSender: ActorRef = sender
      var i:Integer = 0
      var brk:Boolean = true
      for (j <- 0 until fingerTable.length) {
        i = fingerTable.length - 1 -j
        if ((brk) &&((((id < fingerTable(i).successorId) && (fingerTable(i).successorId < fid)) || ((id == fid) && (fingerTable(i).successorId != id)) || ((id > fid) && (((id < fingerTable(i).successorId) && (fingerTable(i).successorId < (fid + cross))) || ((id < (fingerTable(i).successorId + cross)) && ((fingerTable(i).successorId + cross) < (fid + cross)))))))) {
          var finNode: ActorRef = fingerTable(i).successor
          var finNodeId: Integer = fingerTable(i).successorId
          var finNodeSuc: ActorRef = null
          var finNodeSucId: Integer = 0
          brk =false
          mainSender ! both(finNode, finNodeId)
        }

        if (i == 0) {

        }

        Thread.sleep(10)
      }
      if(brk){
        mainSender ! both(self, id)
      }
      

    }
    
    /*
     * returns successor of current node(actor)
     */
    case YourSuccessor => {
      sender ! both(successor, successorId)
    }

    case YourPredecessor =>
      sender ! both(predecessor, predecessorId)

    case YourId =>
      sender ! resId(id)

  }

}

/*
 * Master
 */
class master(numOfNodes: Integer, numOfRequests:Integer, idLimiter: Integer) extends Actor {
  var networkNodes = new ArrayBuffer[ActorRef]
  val byteLimiter = if (idLimiter % 8 > 0) ((idLimiter / 8) + 1) else (idLimiter / 8)
  var idSpace: ArrayBuffer[Integer] = new ArrayBuffer[Integer]
  var nodeMap = scala.collection.mutable.Map[Integer, Integer]()
  var hash: Integer = null
  var tempArray: Array[Integer] = new Array[Integer](numOfNodes)
  var i = 0
  var offset = numOfNodes
  var parentId: Integer = 0
  var requestCount:Integer =0
  var TotalHops:Integer = 0

  def receive = {
    case Start =>
      println("Building the network using Chord Protocol...")
      var index = 0
      var joinDone: Boolean = false
      var joinFinished: Boolean = false
      var count:Integer = 0
      var reqCount:Integer = 0
      for (i <- 0 until numOfNodes) {
        joinDone = false
        index = i
        hash = encodedName(index.toString())
        while (nodeMap.contains(hash)) {
          hash = encodedName(offset.toString())
          index = offset
          offset = offset + 1
        }
        networkNodes += context.actorOf(Props(new Node(index.toString(), hash, byteLimiter * 8)), name = "node" + i)
        if (networkNodes.length == 1) {
          implicit var timeout = Timeout(200)
          var future: Future[String] = ask(networkNodes(i), Initialize).mapTo[String]
          future.onSuccess {
            case "JoinDone" =>
              count = 1
              println("Node 1 joined the system.")
              if(count == numOfNodes){
                println("Number of hops are " + numOfRequests)
                context.system.shutdown()
              }
                
              joinDone = true

          }
          Thread.sleep(200)

        } else {

          val ar: Array[Integer] = nodeMap.keySet.toArray
          parentId = ar(Random.nextInt(ar.length))
          implicit var timeout = Timeout(90000000)
          var future: Future[String] = ask(networkNodes(i), Join(networkNodes(nodeMap(parentId)))).mapTo[String]
          future.onSuccess {
            case "JoinDone" =>
              count = count+1
              println("Node" + count + " joined the system.")
              joinDone = true
              if(count == numOfNodes){
                println("Join finished ...")
                //self ! StartSearch
                context.system.shutdown()
              }

          }
          Thread.sleep(200)
        }
        nodeMap += { hash -> i }
        
      }
      
      
    case StartSearch =>
      val rumourInterval = Duration.create(1000, scala.concurrent.duration.MILLISECONDS)
      context.system.scheduler.scheduleOnce(rumourInterval, self, DoSearch)
      
    case DoSearch =>
      requestCount = requestCount+1
      var reqId:Integer = Random.nextInt(numOfRequests)
      var key:Integer = encodedName("request"+reqId)
      var counter:Integer = 0
      for(i <- 0 until numOfNodes){
        implicit var timeout = Timeout(1000000)
        var future:Future[Found] = ask(networkNodes(i), Find(key)).mapTo[Found]
        future.onSuccess{
          case Found(n:Integer) =>
            counter = counter + 1
            TotalHops = TotalHops+ n
            if((counter == numOfNodes) && (requestCount == numOfRequests)){
              println("Average number of hops : " + (TotalHops/(numOfNodes*numOfRequests)))
              context.system.shutdown()
            }
        }
      }
    
  }

  /*
   * Print All Fingers
   */
  def printFingers = {
    for (i <- 0 until networkNodes.length) {

      implicit var timeout = Timeout(5000)
      var future: Future[String] = ask(networkNodes(i), Print).mapTo[String]
      future.onSuccess {
        case "PrintDone" =>

      }
      Thread.sleep(200)
    }
    context.system.shutdown()
  }

  /*
   * Hash the node
   */
  def encodedName(name: String): Integer = {
    val messageDigest = java.security.MessageDigest.getInstance("SHA-1")
    var temp: Integer = 0
    for (i <- 0 until byteLimiter) {
      temp += (messageDigest.digest(name.getBytes)(i) & 0xFF)

    }
    return temp
  }
}