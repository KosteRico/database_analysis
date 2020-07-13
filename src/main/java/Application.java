import databases.CalciteDatabase;
import databases.Database;
import databases.MapdDatabase;
import databases.PostgresDatabase;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    private static List<String> getTestSqlQueries() {
        List<String> queries = new ArrayList<>();

        queries.add("select * from logs limit 10");
        queries.add("select * from logs limit 100");
//        queries.add("select * from logs limit 1000");
//        queries.add("select * from logs limit 10000");
//        queries.add("select * from logs limit 100000");

        queries.add("select count(*) from logs limit " + Integer.MAX_VALUE);
        queries.add("select count(*), \"problem_number\" from logs group by \"problem_number\" limit " + Integer.MAX_VALUE);
        queries.add("select avg(\"problem_number\") from logs group by \"problem_number\" limit " + Integer.MAX_VALUE);
//        queries.add("select * from logs limit 10");
//        queries.add("select * from logs limit 10");

        return queries;
    }

    private static List<Database> getDatabases() throws SQLException, ClassNotFoundException {
        List<Database> databases = new ArrayList<>();

        databases.add(new MapdDatabase());
        databases.add(new CalciteDatabase());
        databases.add(new PostgresDatabase());

        return databases;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        BasicConfigurator.configure();

        StopWatch stopWatch = new StopWatch();

        List<Database> databases = getDatabases();
        List<String> sqlQueries = getTestSqlQueries();

        logger.debug("Connected");

        PreparedStatement statement;
        FileWriter writer = new FileWriter("results.txt");

        for (String sql : sqlQueries) {
            writer.write(sql + " :\n\n");
            for (Database database : databases) {

                float timeRes = 0.0f;

                final int numResults = 10;

                for (int i = 0; i < numResults; i++) {
                    statement = database.getConnection().prepareStatement(sql);

                    stopWatch.start();
                    statement.execute();
                    stopWatch.stop();

                    timeRes += stopWatch.getTime() / 1000f;

                    statement.close();

                    stopWatch.reset();
                }

                float avgTime = timeRes / numResults;

                writer.write(database.getClass() + "\t" + avgTime + "\n");
            }

            writer.write("\n");
        }

        for (Database database : databases) {
            database.close();
        }

        writer.close();
    }
}
