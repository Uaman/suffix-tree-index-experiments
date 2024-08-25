package com.dima_z.indexes.clickHouse;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.dima_z.indexes.IIndex;
import com.dima_z.pojo.IndexValueObject;

public class ClickHouseIndex implements IIndex {

    private String CLICKHOUSE_HOST = null;
    private String CLICKHOUSE_TABLE_NAME = null;
    private Connection connection = null;
    private int numberOfItems = 0;

    public ClickHouseIndex() {
        System.out.println("ClickHouseIndex");
        this.initFromConfig();

        Properties properties = new Properties();
        properties.put("compress", 0);
        try {
            connection = DriverManager.getConnection(CLICKHOUSE_HOST, properties);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void initFromConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            props.load(input);
            this.CLICKHOUSE_HOST = props.getProperty("clickhouseHost");
            this.CLICKHOUSE_TABLE_NAME = props.getProperty("clickhouseTableName");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public Collection<Integer> search(String query) {
        String searchQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name = '" + query + "'";
        PreparedStatement searchStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            searchStatement = this.connection.prepareStatement(searchQuery);
            resultSet = searchStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            searchStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> includes(String query) {
        String includesQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name LIKE '%" + query + "%'";
        PreparedStatement includesStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            includesStatement = this.connection.prepareStatement(includesQuery);
            resultSet = includesStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            includesStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notIncludes(String query) {
        String notIncludesQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name NOT LIKE '%" + query + "%'";
        PreparedStatement notIncludesStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            notIncludesStatement = this.connection.prepareStatement(notIncludesQuery);
            resultSet = notIncludesStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            notIncludesStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> startsWith(String query) {
        String startsWithQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name LIKE '" + query + "%'";
        PreparedStatement startsWithStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            startsWithStatement = this.connection.prepareStatement(startsWithQuery);
            resultSet = startsWithStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            startsWithStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notStartsWith(String query) {
        String notStartsWithQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name NOT LIKE '" + query + "%'";
        PreparedStatement notStartsWithStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            notStartsWithStatement = this.connection.prepareStatement(notStartsWithQuery);
            resultSet = notStartsWithStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            notStartsWithStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> endsWith(String query) {
        String endsWithQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name LIKE '%" + query + "'";
        PreparedStatement endsWithStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            endsWithStatement = this.connection.prepareStatement(endsWithQuery);
            resultSet = endsWithStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            endsWithStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notEndsWith(String query) {
        String notEndsWithQuery = "SELECT id FROM " + this.CLICKHOUSE_TABLE_NAME + " WHERE name NOT LIKE '%" + query + "'";
        PreparedStatement notEndsWithStatement = null;
        ResultSet resultSet = null;
        Collection<Integer> result = null;
        try {
            notEndsWithStatement = this.connection.prepareStatement(notEndsWithQuery);
            resultSet = notEndsWithStatement.executeQuery();
            result = this.parseResultSet(resultSet);
            notEndsWithStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    public void createIndex() {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + this.CLICKHOUSE_TABLE_NAME + " ("
            + "id Int32, "
            + "name String"
            + ") ENGINE = MergeTree() ORDER BY id";

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createTableQuery);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void dropIndex() {
        String dropTableQuery = "DROP TABLE IF EXISTS " + this.CLICKHOUSE_TABLE_NAME;

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(dropTableQuery);
            preparedStatement.execute();
            preparedStatement.close();
            numberOfItems = 0;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Table dropped");
    }

    public void bulkInsert(List<IndexValueObject> items) {

        try (PreparedStatement ps = connection.prepareStatement(
            "insert into " + this.CLICKHOUSE_TABLE_NAME + " select id, name from input('id Int32, name String')")) {
            // The column definition will be parsed so the driver knows there are 3 parameters: col1, col2 and col3
                for (IndexValueObject item : items) {
                    ps.setInt(1, Integer.parseInt(item.id));
                    ps.setString(2, item.name);
                    ps.addBatch(); // parameters will be write into buffered stream immediately in binary format
                }            
            ps.executeBatch(); // stream everything on-hand into ClickHouse
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }        
    }

    public boolean isEmpty() {
        return numberOfItems == 0;
    }

    private Collection<Integer> parseResultSet(ResultSet resultSet) {
        Collection<Integer> result = new ArrayList<>();
        try {
            while (resultSet.next()) {
                result.add(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    
    public static void main(String[] args) {
        ClickHouseIndex clickHouseIndex = new ClickHouseIndex();
        clickHouseIndex.createIndex();
        // int batchSize = 50000;
        // int indexSize = 50000;
        // int currentIndexSize = 0;
        // String sourceCSVFileName = "./data/geo_dataset_utf8_2M.csv";
        // CSVFileStreamer csvStreamer = new CSVFileStreamer(sourceCSVFileName, batchSize);
        // Iterator<List<IndexValueObject>> iterator = csvStreamer.iterator();
        // List<IndexValueObject> currentBatch = null;

        // while (iterator.hasNext()) {
        //         //System.out.println(">>>>>" + currentIndexSize + " <<<<<");
        //     currentBatch = iterator.next();
        //     clickHouseIndex.bulkInsert(currentBatch);
        //     currentIndexSize += currentBatch.size();
        //     if (currentIndexSize >= indexSize) {
        //         break;
        //     }
        // }

        // Collection result = clickHouseIndex.startsWith("Mas");
        // for (Object id : result) {
        //     System.out.println(id);
        // }
        clickHouseIndex.dropIndex();
    }
}
