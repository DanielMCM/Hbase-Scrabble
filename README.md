# Hbase-Scrabble
This repository contains and example of HBase use based on an Assignment of the course Cloud Computing at university.

# Prerequisites

  - This assumes you have an instance of HBase running on your ubuntu os system --> https://hbase.apache.org/
  - Install Maven --> https://maven.apache.org/
  - The dataset used can be found here --> https://datahub.io/five-thirty-eight/scrabble-games#resource-scrabble_games

# Execution 

 1. Compile the project using Maven from root
 
 ```
 mvn clean install
 ```
 
 2. The following comands can be used where zk_host:zk_port are the host and port of the HBase instance (Localhost + port if used locally):
 
 ```
 bin/HBaseScrabble zk_host:zk_port createTable
 bin/HBaseScrabble zk_host:zk_port loadTable
 bin/HBaseScrabble zk_host:zk_port query1 tourneyId winnerName
 bin/HBaseScrabble zk_host:zk_port query2 firstTouneryId lastTourneyId
 bin/HBaseScrabble zk_host:zk_port query3 tourneyId
 ```

 3. The different queries are:
    - **Query1**: Returns all the opponents (Loserid) of a given Winnername in a tournament (Tourneyid)
    - **Query2**: Returns the ids of the players (winner and loser) that have participated more than once in all tournaments between two given Tourneyids
    - **Query3**: Given a Tourneyid, the query returns the Gameid, the ids of the two participants that have finished in tie
