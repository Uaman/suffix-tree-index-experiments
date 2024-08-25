package com.dima_z.indexes.postgres;

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

public class PostgresIndex implements IIndex {

    private String POSTRGESS_USER = null;
    private String POSTRGESS_PASSWORD = null;
    private String POSTRGESS_SERVER_URL = null;
    private String POSTRGESS_SCHEMA = null;
    private String POSTRGESS_TABLE_NAME = null;

    private Connection postgresConnection = null;
    private int numberOfItems = 0;
    
    public PostgresIndex() {
        this.initFromConfig();
        this.openConnection();
    }

    @Override
    public Collection<Integer> search(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name = '" + query + "'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    @Override
    public Collection<Integer> includes(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name LIKE '%" + query + "%'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    @Override
    public Collection<Integer> notIncludes(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name NOT LIKE '%" + query + "%'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    @Override
    public Collection<Integer> startsWith(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name LIKE '" + query + "%'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    @Override
    public Collection<Integer> notStartsWith(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name NOT LIKE '" + query + "%'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    @Override
    public Collection<Integer> endsWith(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name LIKE '%" + query + "'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    @Override
    public Collection<Integer> notEndsWith(String query) {
        String sql = "SELECT id FROM \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" WHERE name NOT LIKE '%" + query + "'";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.parseResultSet(resultSet);
    }

    public void bulkInsert(List<IndexValueObject> items) {
        String insertQuery = "INSERT INTO \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" (id, name) VALUES (?, ?)";
        PreparedStatement insertStatement = null;
        try {
            this.postgresConnection.setAutoCommit(false);
            insertStatement = this.postgresConnection.prepareStatement(insertQuery);
            for (IndexValueObject item : items) {
                insertStatement.setInt(1, Integer.parseInt(item.id));
                insertStatement.setString(2, item.name);
                insertStatement.addBatch();
            }
            insertStatement.executeBatch();
            this.postgresConnection.commit();
            this.numberOfItems += items.size();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (insertStatement != null) {
                try {
                    insertStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            try {
                this.postgresConnection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isEmpty() {
        return numberOfItems == 0;
    }
    
    private void closeConnection() {
        try {
            postgresConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initFromConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            props.load(input);
            this.POSTRGESS_SCHEMA = props.getProperty("postgresSchema");
            this.POSTRGESS_TABLE_NAME = props.getProperty("postgresTableName");
            this.POSTRGESS_SERVER_URL = props.getProperty("postgresServerUrl");
            this.POSTRGESS_USER = props.getProperty("postgresUser");
            this.POSTRGESS_PASSWORD = props.getProperty("postgresPassword");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            postgresConnection = DriverManager.getConnection(
                POSTRGESS_SERVER_URL, // Database URL
                POSTRGESS_USER, // Database username
                POSTRGESS_PASSWORD  // Database password
            );
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Collection<Integer> parseResultSet(ResultSet resultSet) {
        Collection<Integer> result = new ArrayList<>();
        try {
            while (resultSet.next()) {
                result.add(resultSet.getInt("id"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void dropIndex() {
        String dropTableQuery = "DROP TABLE IF EXISTS \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\"";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(dropTableQuery);
            preparedStatement.execute();
            numberOfItems = 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createIndex() {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS \"" + POSTRGESS_SCHEMA + "\".\"" + POSTRGESS_TABLE_NAME + "\" ("
        + "id INT NOT NULL PRIMARY KEY,"
        + "name VARCHAR(255) NOT NULL"
        + ")";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = postgresConnection.prepareStatement(createTableQuery);
            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PostgresIndex postgresIndex = new PostgresIndex();
        //Collection<Integer> result = postgresIndex.includes("Rock Island");
        //postgresIndex.createIndex();
        postgresIndex.dropIndex();
        postgresIndex.closeConnection();
        
        //System.out.println("Result:" + result.size());
    }
}
