package databases;

import java.sql.Connection;
import java.sql.SQLException;

public class CalciteDatabase extends Database {

    public CalciteDatabase() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");

        this.setConnection();
    }

    @Override
    protected String getUsername() {
        return "admin";
    }

    @Override
    protected String getPassword() {
        return "admin";
    }

    @Override
    protected String getConnectionString() {
        String modelPath = System.getenv("model_path");

        return "jdbc:calcite:model=" + modelPath;
    }

    public Connection getConnection() {
        return this.connection;
    }

}
