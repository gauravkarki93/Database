import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;

public class DB_Helper {
    Connection connection;

    public DB_Helper()
    {
        connection = null;
    }

    public static void main(String[] args) {
        DB_Helper db = new DB_Helper();
        db.getConnection();
    }

    public Connection getConnection(){

        try{
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException e)
        {
            System.out.println("Error loading driver: " +e);
            return null;
        }

        String host = "localhost";
        String dbName = "imdbcoen280";
        int port = 1521;
        String oracleURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
        String username = "gaurav";
        String password = "jinglebell";

        try{
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            connection = DriverManager.getConnection(oracleURL, username, password);
            System.out.println(connection);
        }
        catch (SQLException e)
        {
            System.out.println("Unable to get connection: " +e);
            return null;
        }
        return connection;
    }

    public void destroyConnection(Connection con)
    {
        try {
            if(con != null)
            {
                con.close();
                System.out.println("Connection destroyed");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
