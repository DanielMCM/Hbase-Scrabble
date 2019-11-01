# Hbase-Scrabble
This repository contains and example of HBase use based on an Assignment of the course Cloud Computing at university.

# Prerequisites

  - This assumes you have an instance of HBase running on your ubuntu os system --> https://hbase.apache.org/
  - Install Maven --> https://maven.apache.org/
  - The dataset used can be found here --> https://datahub.io/five-thirty-eight/scrabble-games#resource-scrabble_games

# Execution 

 1. Compile the project using from root
 
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
