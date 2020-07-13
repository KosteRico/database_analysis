package databases;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class Database implements Closeable {

    protected Connection connection;

    protected static final String HOST = System.getenv("host");

    protected abstract String getUsername();

    protected abstract String getPassword();

    protected abstract String getConnectionString();

    public Connection getConnection() {
        return connection;
    }

    protected void setConnection() throws SQLException {
        this.connection = DriverManager.getConnection(getConnectionString(), getUsername(), getPassword());
    }

    @Override
    public void close() throws IOException {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e.getCause());
        }
    }
}
