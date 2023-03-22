package galina;

import java.sql.*;

public class App {
    public static void main(String[] args) {
        String DB_URL     = "jdbc:postgresql://rc1b-h2ibywz9kbvpjomr.mdb.yandexcloud.net:6432/meteo?targetServerType=master&ssl=true&sslmode=verify-full";
        String DB_USER    = "meteo";
        String DB_PASS    = "meteometeo";

        try {
            Class.forName("org.postgresql.Driver");

            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            ResultSet q = conn.createStatement().executeQuery("SELECT version()");
            if(q.next()) {System.out.println(q.getString(1));}

            conn.close();
        }
        catch(Exception ex) {ex.printStackTrace();}
    }
}
