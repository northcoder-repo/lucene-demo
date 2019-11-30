package org.northcoder.titlelookupservice;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public enum DbConn {

    INST;
    
    private Connection conn = null;

    DbConn() {
        try {
            final String jdbcDriver = "com.mysql.cj.jdbc.Driver";
            final String dbUrl = "jdbc:mysql://localhost:3306/your_schema_here";
            final String user = "your_user_id_here";
            final String pass = "your_password_here";
            Class.forName(jdbcDriver);
            try {
                conn = DriverManager.getConnection(dbUrl, user, pass);
            } catch (SQLException ex) {
                Logger.getLogger(DbConn.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DbConn.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Connection getDbConn() {
        return conn;
    }

}
