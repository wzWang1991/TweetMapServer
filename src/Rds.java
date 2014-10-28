import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;


public class Rds {
    final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    final String DB_URL = "jdbc:mysql://tweetmap.crsarl5br9bw.us-east-1.rds.amazonaws.com:3306/tweet";
    Connection conn;
    
    private static Rds instance = null;
    private Rds() {
    	conn = null;
    }
    
    public static Rds getInstance() {
    	if (instance == null)
    		instance = new Rds();
    	return instance;
    }
    
    public boolean isConnected() {
    	return conn != null;
    }
    
    public void init(String password) {
        try {
        	if (conn == null) {
                Class.forName(JDBC_DRIVER);
                System.out.println("Connecting to database...");
                conn = DriverManager.getConnection(DB_URL, "xiaojing", password);
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
    
    private void createTable(String name) {
        System.out.println("Creating table in given database...");
        Statement stmt;
        try {
            stmt = conn.createStatement();
            String sql = "CREATE TABLE " +name+ " "+
                    "(id_str VARCHAR(255) not NULL, " +
                    " text VARCHAR(2000), " +
                    " coor1 VARCHAR(255), " +
                    " coor2 VARCHAR(255), " +
                    " created_at VARCHAR(255), " +
                    " PRIMARY KEY ( id_str ))";
            stmt.executeUpdate(sql);
            stmt.close();
            System.out.println("Finished creating table");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void deleteTable(String name) {
        System.out.println("Deleting table in given database...");
        Statement stmt;
        try {
            stmt = conn.createStatement();
            String sql = "DROP TABLE " + name;
            stmt.executeUpdate(sql);
            stmt.close();
            System.out.println("Finished deleting table");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    static HashMap<String, String> map = new HashMap<String, String>();

    private void createMap() {
        map.put("Jan", "01");
        map.put("Feb", "02");
        map.put("Mar", "03");
        map.put("Apr", "04");
        map.put("May", "05");
        map.put("Jun", "06");
        map.put("Jul", "07");
        map.put("Aug", "08");
        map.put("Sep", "09");
        map.put("Oct", "10");
        map.put("Nov", "11");
        map.put("Dec", "12");
    }

    private String convertTime(String date) {
        String processed = null;

        if(map.size()==0){
            createMap();
        }

        // hard coded according to tweet format
        String[] s = date.split(" ");
        String year = s[5];
        String month = s[1];
        String day = s[2];
        String time = s[3];
        processed = year+"-"+map.get(month)+"-"+day+" "+time;

        Timestamp timestamp = Timestamp.valueOf(processed);
        return String.valueOf(timestamp.getTime());
    }

    public List<SelectResult> select(String table, String start, String end) {
        String sql = "SELECT * FROM "+table+" WHERE created_at < '"+end+"' AND created_at > '"+start+"'";
//    	String selectExpression = "select * from " + table + " where created_at > '"+start+"' and created_at < '"+end+"'";
        StringBuilder sb = new StringBuilder();
        Statement stmt;
        int count = 0;
        List<SelectResult> list = new LinkedList<SelectResult>();
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()){
                Gson gson = new Gson();
                SelectResult sr = new SelectResult(rs.getString("id_str"));

                String text = rs.getString("text");
                String c1 = rs.getString("coor1");
                String c2 = rs.getString("coor2");
                String time = rs.getString("created_at");

                sr.setText(text);
                sr.setCoor1(c1);
                sr.setCoor2(c2);
                sr.setTime(time);

                list.add(sr);
                count++;
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println(count);
        return list;

    }

    private void insert(String file, String table) {
        System.out.println("Inserting into table " +table );
        Statement stmt;
        BufferedReader br;

        try {
            stmt = conn.createStatement();
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                Gson gson = new Gson();
                System.out.println("                 "+line);
                Tweet tweet = gson.fromJson(line, Tweet.class);

                Tweet.Coordinate coor = tweet.coordinates;
                String coors = coor.toString();
                String[] ss = coors.split(",");
                String c1 = ss[0].substring(1);
                String c2 = ss[1].substring(1, ss[1].length()-1);
                String timestamp = convertTime(tweet.created_at);

                String text = tweet.text.replaceAll("\u0027", "'\'");
                System.out.println(text);
                String sql = "INSERT INTO " +table +
                        " VALUES ('"+tweet.id_str+"', '"+text+"', '"+c1+"', '"+c2+"', '"+timestamp+"')";

                stmt.executeUpdate(sql);

                line = br.readLine();
            }

            stmt.close();
            System.out.println("Finished inserting into table");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }
}