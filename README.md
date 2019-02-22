# PAXOS-vs-Dynamo
## Distributed Systems Algorithms Final Project

Implementation of a key-value store using two different consistency models.
Strong consistency model with Paxos and eventual consistency model with Dynamo.

The system allows using both models simultaneously by having clients and servers of each type.

It was implemented using the **Akka Framework** in Scala.



### PAXOS
The version of Paxos implemented is Multi-Paxos.

### Dynamo
It was implemented an algorithm based on Amazon Dynamo
- Operations are done in quorums of replicas
- Consistent hashing is used to calculate where keys should be replicated
- CRDT sets (Conflict-free Replicated Data Type), specifically OR-sets, are used to combine results into a consistent state

Each operation was timed by the clients and sent to the Statistics actor, where .dat files were composed and later analysed with a python script.
