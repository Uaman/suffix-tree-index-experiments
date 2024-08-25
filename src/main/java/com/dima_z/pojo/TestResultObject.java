package com.dima_z.pojo;

import com.dima_z.utils.IWritable;

public class TestResultObject implements IWritable {
    
    private String dbName;
    private String querySubtype;
    private String queryType;
    private String query;
    private long duration;
    private int resultSize;
    private int dataSize;
    
    public TestResultObject(String dbName, String queryType, String querySubtype, String query, long duration, int resultSize, int dataSize) {
        this.dbName = dbName;
        this.queryType = queryType;
        this.querySubtype = querySubtype;
        this.query = query;
        this.duration = duration;
        this.resultSize = resultSize;
        this.dataSize = dataSize;
    }

    public String getQueryType() {
        return queryType;
    }
    
    public String getQuery() {
        return query;
    }
    
    public long getDuration() {
        return duration;
    }
    
    public int getResultSize() {
        return resultSize;
    }

    public String getQuerySubtype() {
        return querySubtype;
    }

    public String getDbName() {
        return dbName;
    }

    public int getDataSize() {
        return dataSize;
    }

    @Override
    public String toString() {
        return ">>>>" +
                "dbName=" + dbName +
                "query=" + '"' + query + '"' +
                ", queryType=" + queryType +
                ", querySubtype=" + querySubtype +
                ", duration=" + duration + " ns" +
                ", resultSize=" + resultSize +
                ", dataSize=" + dataSize +
                "<<<<\n";
    }

    public String toCSV() {
        return dbName + "," + queryType + "," + querySubtype + "," + query + "," + duration + "," + resultSize + "," + dataSize + "\n";
    }
}
