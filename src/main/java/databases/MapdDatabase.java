package databases;

import java.sql.SQLException;

public class MapdDatabase extends Database {

    public MapdDatabase() throws ClassNotFoundException, SQLException {
        Class.forName("com.omnisci.jdbc.OmniSciDriver");
        setConnection();
    }

    @Override
    protected String getUsername() {
        return "admin";
    }

    @Override
    protected String getPassword() {
        return "HyperInteractive";
    }

    @Override
    protected String getConnectionString() {
        return "jdbc:omnisci:" + HOST + ":6274:omnisci";
    }
}
