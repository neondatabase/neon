import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Example
{
    public static void main( String[] args ) throws Exception
    {
        String host = System.getenv("NEON_HOST");
        String database = System.getenv("NEON_DATABASE");
        String user = System.getenv("NEON_USER");
        String password = System.getenv("NEON_PASSWORD");

        String url = "jdbc:postgresql://%s/%s".formatted(host, database);
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        Connection conn = DriverManager.getConnection(url, props);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1");
        while (rs.next())
        {
            System.out.println(rs.getString(1));
        }
        rs.close();
        st.close();
    }
}
