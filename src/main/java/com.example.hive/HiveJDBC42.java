package com.example.hive;

import java.sql.*;
import com.cloudera.hive.jdbc.HS2Driver;


public class HiveJDBC42 {
    private static String driver = "com.cloudera.hive.jdbc.HS2Driver";

    public static void main(String[] args) throws SQLException {
        String      statement = "SHOW DATABASES";
        String      jdbcStr   = "jdbc:hive2://c1402-node9.coelab.cloudera.com:10000/default;ssl=true;sslTrustStore=cm-auto-global_truststore.jks;logLevel=6;logPath=/tmp/";
        String      username  = "hive";
        String      password  = "hive";

        if (args.length > 0 ) {
            statement = args[0];
            if (args.length > 1 ) jdbcStr  = args[1];
            if (args.length > 2 ) username = args[2];
            if (args.length > 3 ) password = args[3];
        }

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection connect = DriverManager.getConnection(jdbcStr, username, password);
        Statement state = connect.createStatement();

        // Run statement
        System.out.println("Executing on HiveJDBC42: " + statement);
        try {
            ResultSet res = state.executeQuery(statement);
            int rows=0;
            while (res.next()) {
                if (res.getMetaData().getColumnCount()>0) {
                    for (int i = 1; i <= res.getMetaData().getColumnCount(); i++) {
                        System.out.print(res.getString(i) + "\t");
                    }
                    System.out.print("\n");
                    rows++;
                }
            }
            System.out.println("(" + rows + ") rows");
        } catch (SQLException e){
            e.printStackTrace();
        }
    }
}

