import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.*;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import java.lang.*;

public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private byte[] table = Bytes.toBytes("ScrabbleGames");

    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */


    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    public void createTable() throws IOException {
        HTableDescriptor hTable = new HTableDescriptor(table);
        hTable.addFamily(new HColumnDescriptor("info"));
        hTable.addFamily(new HColumnDescriptor("winner"));
        hTable.addFamily(new HColumnDescriptor("loser"));
        this.hBaseAdmin.createTable(hTable);
    }

    public void loadTable(String folder)throws IOException{
    	HTable hTable = new HTable(config,table);
    	BufferedReader br = new BufferedReader(new FileReader(folder+"scrabble_games.csv"));
    	String line = br.readLine();
    	int i = 1;
    	while ((line = br.readLine()) != null) {	
    		String[] values = line.split(",");
    		Put p = new Put(Bytes.toBytes("row" + i));
    		p.add(Bytes.toBytes("info"), Bytes.toBytes("gameid"), Bytes.toBytes(values[1]));
    		p.add(Bytes.toBytes("info"), Bytes.toBytes("tourneyid"), Bytes.toBytes(values[2]));
    		p.add(Bytes.toBytes("info"), Bytes.toBytes("tie"), Bytes.toBytes(values[3]));
    		p.add(Bytes.toBytes("winner"), Bytes.toBytes("winnerid"), Bytes.toBytes(values[4]));
    		p.add(Bytes.toBytes("winner"), Bytes.toBytes("winnername"), Bytes.toBytes(values[5]));
    		p.add(Bytes.toBytes("winner"), Bytes.toBytes("winnerscore"), Bytes.toBytes(values[6]));
    		p.add(Bytes.toBytes("winner"), Bytes.toBytes("winneroldrating"), Bytes.toBytes(values[7]));
    		p.add(Bytes.toBytes("winner"), Bytes.toBytes("winnernewrating"), Bytes.toBytes(values[8]));
    		p.add(Bytes.toBytes("winner"), Bytes.toBytes("winnerpos"), Bytes.toBytes(values[9]));
    		p.add(Bytes.toBytes("loser"), Bytes.toBytes("loserid"), Bytes.toBytes(values[10]));
    		p.add(Bytes.toBytes("loser"), Bytes.toBytes("loserscore"), Bytes.toBytes(values[11]));
    		p.add(Bytes.toBytes("loser"), Bytes.toBytes("loseroldrating"), Bytes.toBytes(values[12]));
    		p.add(Bytes.toBytes("loser"), Bytes.toBytes("losernewrating"), Bytes.toBytes(values[13]));
			p.add(Bytes.toBytes("loser"), Bytes.toBytes("loserpos"), Bytes.toBytes(values[14]));
			p.add(Bytes.toBytes("info"), Bytes.toBytes("round"), Bytes.toBytes(values[15]));
			p.add(Bytes.toBytes("info"), Bytes.toBytes("division"), Bytes.toBytes(values[16]));
			p.add(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(values[17]));
			p.add(Bytes.toBytes("info"), Bytes.toBytes("lexicon"), Bytes.toBytes(values[18]));
			hTable.put(p);
			i++;
    	}
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }



    public List<String> query1(String tourneyid, String winnername) throws IOException {
    	HTable hTable = new HTable(config,table);
    	Scan scan = new Scan();
    	scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tourneyid"));
    	scan.addColumn(Bytes.toBytes("winner"), Bytes.toBytes("winnername"));
    	scan.addColumn(Bytes.toBytes("loser"), Bytes.toBytes("loserid"));
    	Filter f1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("tourneyid"), CompareFilter.CompareOp.EQUAL, tourneyid.getBytes());
    	Filter f2 = new SingleColumnValueFilter(Bytes.toBytes("winner"), Bytes.toBytes("winnername"), CompareFilter.CompareOp.EQUAL, winnername.getBytes());
    	FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    	filterList.addFilter(f1);
    	filterList.addFilter(f2);
    	scan.setFilter(filterList);
    	ResultScanner scanner = hTable.getScanner(scan);
    	List<String> query1 = new ArrayList<String>();
    	
    	for(Result result = scanner.next(); result != null; result = scanner.next()) {
    		byte [] value = result.getValue(Bytes.toBytes("loser"), Bytes.toBytes("loserid"));
    		String aux = new String(value);
    		query1.add(aux);
    	}
    	
        return query1;

    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public List<String> query3(String tourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }


    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseScrabble.createTable();
        }
        else if(args[1].toUpperCase().equals("LOADTABLE")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
            System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
            System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
            System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
        }
        else{
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }



}
