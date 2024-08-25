package com.dima_z.indexes.indexFactory;

public enum IndexType {
    SUFFIX_TREE("suffixTree"),
    ELASTIC_TREE("elastic"),
    MYSQL_TREE("mysql"),
    POSTGRES_TREE("postgres"),
    CLICKHOUSE_TREE("clickhouse");

    private final String type;

    IndexType(String val) {
        this.type = val;
    }

    public String getType() {
        return type;
    }
}