import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.*;
/**
 * @author RJY
 * @version 1.0.0
 * A class which reads data from HDFS and sort-merge join the records and store the result into HBase.
 */
public class Hw1Grp1 {

    public static void main(String args[]) throws IOException,MasterNotRunningException,ZooKeeperConnectionException,URISyntaxException{

        /**
         * @param file receive the path of table R
         * @param file1 receive the path of table S
         * @param keyR receive the row number of join key of table R
         * @param keyS receive the row number of join key of table S
         * @param valueR receive the row number of value in table R
         * @param valueS receive the row number of value in table S
         * @param column_keyR receive the column key of table R
         * @param column_keyS receive the column key of table S
         * @throws IOException,MasterNotRunningException,ZooKeeperConnectionException,URISyntaxException in case of any kind of failure
         */

        String const_str = "hdfs://localhost:9000";
        String file= const_str + args[0].substring(2);
        String file1= const_str + args[1].substring(2);
        int keyR = Integer.parseInt(args[2].substring(6, 7));
        int keyS = Integer.parseInt(args[2].substring(9, 10));
        int valueR = Integer.parseInt(args[3].substring(5, 6));
        int valueS = Integer.parseInt(args[3].substring(8, 9));
        String value_of_column = args[3].substring(4);
        String[] column_key = value_of_column.split(",");
        String column_keyR = column_key[0];
        String column_keyS = column_key[1];

        List<String> column_key_session = new ArrayList<String>();//store the real columnkey which is about to store in HBase
        List<String> value_session = new ArrayList<String>();//store the value
        List<String> row_key = new ArrayList<String>();//store the row key

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;//store data temporarily
        List<String[]> sortR = new ArrayList<String[]>();//store the strings from file
        while((s=in.readLine())!=null) {//add the strings which are read from file to the arraylist sortR
            sortR.add(s.split("\\|"));
        }

        String [] temp;
        for(int i=0;i<sortR.size()-1;i++){//this is the procedures of bubble sort which sort the arraylist sortR
            for(int j=0;j<sortR.size()-i-1;j++){
                if(sortR.get(j)[keyR].compareTo(sortR.get(j+1)[keyR])>0){
                    temp = sortR.get(j);
                    sortR.set(j,sortR.get(j+1));
                    sortR.set(j+1,temp);
                }
            }
        }

        in.close();
        fs.close();

        Configuration conf1 = new Configuration();
        FileSystem fs1 = FileSystem.get(URI.create(file1), conf1);
        Path path1 = new Path(file1);
        FSDataInputStream in_stream1 = fs1.open(path1);

        BufferedReader in1 = new BufferedReader(new InputStreamReader(in_stream1));
        String s1;//store Data temporarily
        List<String[]> sortS = new ArrayList<String[]>();//store the strings from file1
        while((s1=in1.readLine())!=null) {//add the strings which are read from file to the arraylist sortS
            sortS.add(s1.split("\\|"));
        }

        String [] temp1;
        for(int i=0;i<sortS.size()-1;i++){//this is the procedures of bubble sort which sort the arraylist sortS
            for(int j=0;j<sortS.size()-i-1;j++){
                if(sortS.get(j)[keyS].compareTo(sortS.get(j+1)[keyS])>0){
                    temp1 = sortS.get(j);
                    sortS.set(j,sortS.get(j+1));
                    sortS.set(j+1,temp1);
                }
            }
        }

        in1.close();
        fs1.close();

        Logger.getRootLogger().setLevel(Level.WARN);

        // create table descriptor
        String tableName= "Result";
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);

        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);

        if (hAdmin.tableExists(tableName)) {
            System.out.println("Table already exists");
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("Old table "+tableName+ " deleted successfully");
        }
        hAdmin.createTable(htd);
        System.out.println("Table "+tableName+ " created successfully");
        hAdmin.close();

        /*
         * sort-merge join
         */
        int r = 0;
        int q = 0;
        int count_q = 0;//this is used to generate the small label, like ".1" in "S2.1"
        int count_r = 0;//this is used to generate the small label, like ".1" in "R2.1"
        boolean flag1 = false;//this is uesd to flag if the last loop enter in line 190
        boolean flag2 = false;//this is uesd to flag if the last loop enter in line 209

        while(r<sortR.size() && q<sortS.size()){
            if(sortR.get(r)[keyR].compareTo(sortS.get(q)[keyS])>0){//when the rth tuple in table R is less than the qth tuple in table S
                flag1 = false;
                flag2 = false;
                q++;
                count_q = 0;
                count_r = 0;
            }else if(sortR.get(r)[keyR].compareTo(sortS.get(q)[keyS])<0){//when the rth tuple in table R is larger than the qth tuple in table S
                flag1 = false;
                flag2 = false;
                r++;
                count_q = 0;
                count_r = 0;
            }else{//when the rth tuple in table R is equal to the qth tuple in table S, find matched tuples

                if(flag1 && flag2){//if both of two flags are true
                    //increase the two smal labels
                    count_r = count_r + 1;
                    count_q = count_q + 1;

                    //store two tuples to the two arraylists
                    row_key.add(sortR.get(r)[keyR]);
                    column_key_session.add(column_keyR+"."+Integer.toString(count_r));//add extra small label to the column key
                    value_session.add(sortR.get(r)[valueR]);
                    row_key.add(sortS.get(q)[keyS]);
                    column_key_session.add(column_keyS+"."+Integer.toString(count_q));//add extra small label to the column key
                    value_session.add(sortS.get(q)[valueS]);
                }else{
                    //reset the two small labels to zero
                    count_r = 0;
                    count_q = 0;

                    //store two tuples to the two arraylists
                    row_key.add(sortR.get(r)[keyR]);
                    column_key_session.add(column_keyR);
                    value_session.add(sortR.get(r)[valueR]);
                    row_key.add(sortS.get(q)[keyS]);
                    column_key_session.add(column_keyS);
                    value_session.add(sortS.get(q)[valueS]);
                }

                int q_temp;
                q_temp = q+1;
                while(q_temp<sortS.size() && sortR.get(r)[keyR].compareTo(sortS.get(q_temp)[keyS]) == 0){
                    flag1 = true;

                    //increase the two smal labels
                    count_r = count_r + 1;
                    count_q = count_q + 1;

                    //find match and store two tuples to the two arraylists
                    row_key.add(sortR.get(r)[keyR]);
                    column_key_session.add(column_keyR+"."+Integer.toString(count_r));//add extra small label to the column key
                    value_session.add(sortR.get(r)[valueR]);
                    row_key.add(sortS.get(q_temp)[keyS]);
                    column_key_session.add(column_keyS+"."+Integer.toString(count_q));//add extra small label to the column key
                    value_session.add(sortS.get(q_temp)[valueS]);
                    q_temp++;
                }

                int r_temp;
                r_temp = r+1;
                while(r_temp<sortR.size() && sortR.get(r_temp)[keyR].compareTo(sortS.get(q)[keyS]) == 0){
                    flag2 = true;

                    //increase the two smal labels
                    count_r = count_r + 1;
                    count_q = count_q + 1;

                    //find match and store two tuples to the two arraylists
                    row_key.add(sortR.get(r_temp)[keyR]);
                    column_key_session.add(column_keyR+"."+Integer.toString(count_r));//add extra small label to the column key
                    value_session.add(sortR.get(r_temp)[valueR]);
                    row_key.add(sortS.get(q)[keyS]);
                    column_key_session.add(column_keyS+"."+Integer.toString(count_q));//add extra small label to the column key
                    value_session.add(sortS.get(q)[valueS]);
                    r_temp++;
                }
                r++;
                q++;
            }
        }

        //write all the matched tuples into HBase
        HTable table = new HTable(configuration,tableName);
        for(int i=0;i<row_key.size();i++){
            Put put = new Put(row_key.get(i).getBytes());
            put.add("res".getBytes(),column_key_session.get(i).getBytes(),value_session.get(i).getBytes());
            table.put(put);
            //System.out.println("put successfully");
        }
        table.close();
        System.out.println("put successfully");
    }
}
