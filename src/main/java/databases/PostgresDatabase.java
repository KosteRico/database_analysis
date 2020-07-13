package databases;

import java.sql.SQLException;

public class PostgresDatabase extends Database {

    public PostgresDatabase() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");

        setConnection();
    }

    @Override
    protected String getUsername() {
        return "postgres";
    }

    @Override
    protected String getPassword() {
        return "upibaz22";
    }

    @Override
    protected String getConnectionString() {
        return "jdbc:postgresql://" + HOST + ":5432/postgres";
    }

}
