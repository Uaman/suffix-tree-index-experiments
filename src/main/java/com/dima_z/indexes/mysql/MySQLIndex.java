package com.dima_z.indexes.mysql;

import java.io.IOException;
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

public class MySQLIndex implements IIndex {

    private String MYSQL_SERVER_URL;
    private String MYSQL_USER; //mysqlUser;
    private String MYSQL_PASSWORD; //mysqlPassword;
    private String MYSQL_TABLE_NAME; //mysqlTableName;
    private Connection mySqlConnection;
    private int numberOfItems = 0;

    public MySQLIndex() {
        System.out.println("MySQLIndex");
        try {
            initFromConfig();
            openConnection();        
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Collection<Integer> search(String query) {
        Collection<Integer> result = null;
        String searchQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name = ?";
        PreparedStatement searchStatement = null;
        try {
            searchStatement = this.mySqlConnection.prepareStatement(searchQuery);
            searchStatement.setString(1, query);
            ResultSet queryResultSet = searchStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            searchStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> includes(String query) {
        Collection<Integer> result = null;
        String includesQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name LIKE '%" + query + "%'";
        PreparedStatement includesStatement = null;
        try {
            includesStatement = this.mySqlConnection.prepareStatement(includesQuery);
            ResultSet queryResultSet = includesStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            includesStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notIncludes(String query) {
        Collection<Integer> result = null;
        String notIncludesQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name NOT LIKE '%" + query + "%'";
        PreparedStatement notIncludesStatement = null;
        try {
            notIncludesStatement = this.mySqlConnection.prepareStatement(notIncludesQuery);
            ResultSet queryResultSet = notIncludesStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            notIncludesStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> startsWith(String query) {
        Collection<Integer> result = null;
        String startsWithQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name LIKE '" + query + "%'";
        PreparedStatement startsWithStatement = null;
        try {
            startsWithStatement = this.mySqlConnection.prepareStatement(startsWithQuery);
            ResultSet queryResultSet = startsWithStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            startsWithStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notStartsWith(String query) {
        Collection<Integer> result = null;
        String notStartsWithQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name NOT LIKE '" + query + "%'";
        PreparedStatement notStartsWithStatement = null;
        try {
            notStartsWithStatement = this.mySqlConnection.prepareStatement(notStartsWithQuery);
            ResultSet queryResultSet = notStartsWithStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            notStartsWithStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> endsWith(String query) {
        Collection<Integer> result = null;
        String endsWithQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name LIKE '%" + query + "'";
        PreparedStatement endsWithStatement = null;
        try {
            endsWithStatement = this.mySqlConnection.prepareStatement(endsWithQuery);
            ResultSet queryResultSet = endsWithStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            endsWithStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notEndsWith(String query) {
        Collection<Integer> result = null;
        String notEndsWithQuery = "SELECT id FROM " + this.MYSQL_TABLE_NAME + " WHERE name NOT LIKE '%" + query + "'";
        PreparedStatement notEndsWithStatement = null;
        try {
            notEndsWithStatement = this.mySqlConnection.prepareStatement(notEndsWithQuery);
            ResultSet queryResultSet = notEndsWithStatement.executeQuery();
            result = processQueryResult(queryResultSet);
            notEndsWithStatement.close();
            queryResultSet.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    public void bulkInsert(List<IndexValueObject> items) {
        System.out.println("bulkInsert " + items.size());
        String insertQuery = "INSERT INTO " + this.MYSQL_TABLE_NAME + " (id, name) VALUES (?, ?)";
        PreparedStatement insertStatement = null;
        try {
            this.mySqlConnection.setAutoCommit(false);
            insertStatement = this.mySqlConnection.prepareStatement(insertQuery);
            for (IndexValueObject item : items) {
                insertStatement.setInt(1, Integer.parseInt(item.id));
                insertStatement.setString(2, item.name);
                insertStatement.addBatch();
            }
            insertStatement.executeBatch();
            this.mySqlConnection.commit();
            this.numberOfItems += items.size();
            insertStatement.close();
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
                this.mySqlConnection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void initFromConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            props.load(input);

            MYSQL_SERVER_URL = props.getProperty("mysqlServerUrl");
            MYSQL_USER = props.getProperty("mysqlUser");
            MYSQL_PASSWORD = props.getProperty("mysqlPassword");
            MYSQL_TABLE_NAME = props.getProperty("mysqlTableName");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void openConnection() {
        try {
            mySqlConnection = DriverManager.getConnection(
                MYSQL_SERVER_URL, // Database URL
                MYSQL_USER, // Database username
                MYSQL_PASSWORD  // Database password
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void closeConnection() {
       try {
            this.mySqlConnection.close();
            this.mySqlConnection = null;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void createIndex() {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + this.MYSQL_TABLE_NAME + " ("
            + "id INT NOT NULL AUTO_INCREMENT,"
            + "name VARCHAR(255) NOT NULL,"
            + "PRIMARY KEY (id)"
            + ")";
        PreparedStatement createTableStatement = null;
        try {
            createTableStatement = this.mySqlConnection.prepareStatement(createTableQuery);
            createTableStatement.execute();
            createTableStatement.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void dropIndex() {
        String dropTableQuery = "DROP TABLE IF EXISTS " + this.MYSQL_TABLE_NAME;
        PreparedStatement dropTableStatement = null;
        try {
            dropTableStatement = this.mySqlConnection.prepareStatement(dropTableQuery);
            dropTableStatement.execute();
            dropTableStatement.close();
            this.numberOfItems = 0;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public boolean isEmpty() {
        return this.numberOfItems == 0;
    }

    private Collection<Integer> processQueryResult(ResultSet resultSet) {
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
        MySQLIndex mysqlIndex = new MySQLIndex();
        //mysqlIndex.createIndex();
        mysqlIndex.dropIndex();
        mysqlIndex.closeConnection();
    }
}
