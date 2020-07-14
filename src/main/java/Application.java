import databases.CalciteDatabase;
import databases.Database;
import databases.MapdDatabase;
import databases.PostgresDatabase;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
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

    private static String formatSqlWithTable(String sql) {
        return String.format(sql, System.getenv("table"));
    }

    private static List<String> getTestSqlQueries() {
        List<String> queries = new ArrayList<>();

        queries.add(formatSqlWithTable("select * from %s limit 10"));
        queries.add(formatSqlWithTable("select * from %s limit 100"));
        queries.add(formatSqlWithTable("select * from %s limit 1000"));
        queries.add(formatSqlWithTable("select * from %s limit 10000"));
        queries.add(formatSqlWithTable("select * from %s limit 100000"));

        queries.add(formatSqlWithTable("select count(*) from %s limit " + Integer.MAX_VALUE));
        queries.add(formatSqlWithTable("select count(*), \"problem_number\" from %s group by \"problem_number\" limit " + Integer.MAX_VALUE));
        queries.add(formatSqlWithTable("select avg(\"problem_number\") from %s group by \"problem_number\" limit " + Integer.MAX_VALUE));

        return queries;
    }

    private static List<Database> getDatabases() throws SQLException, ClassNotFoundException {
        List<Database> databases = new ArrayList<>();

        databases.add(new MapdDatabase());
        databases.add(new PostgresDatabase());
        databases.add(new CalciteDatabase());

        return databases;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        BasicConfigurator.configure();

        Logger.getRootLogger().setLevel(Level.INFO);

        StopWatch stopWatch = new StopWatch();

        List<Database> databases = getDatabases();
        List<String> sqlQueries = getTestSqlQueries();

        logger.info("Connected");

        PreparedStatement statement;
        FileWriter writer = new FileWriter("results.txt");

        for (String sql : sqlQueries) {
            writer.write(sql + " :\n\n");

            logger.info(sql);

            for (Database database : databases) {

                float timeRes = 0.0f;

                final int numResults = 10;

                logger.info(database.getClass() + ":");

                for (int i = 0; i < numResults; i++) {
                    statement = database.getConnection().prepareStatement(sql);

                    stopWatch.start();
                    statement.execute();
                    stopWatch.stop();

                    timeRes += stopWatch.getTime() / 1000f;

                    statement.close();

                    logger.info(i + 1);

                    stopWatch.reset();
                }

                float avgTime = timeRes / numResults;

                String avgTimeStr = Float.toString(avgTime).replace('.', ',');

                writer.write(database.getClass() + "\t" + avgTimeStr + "\n");
            }

            writer.write("\n");
        }

        for (Database database : databases) {
            database.close();
        }

        writer.close();

        logger.info("Done!!!");
    }
}
