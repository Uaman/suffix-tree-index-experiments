package com.dima_z.indexes.indexFactory;

import com.dima_z.indexes.IIndex;
import com.dima_z.indexes.clickHouse.ClickHouseIndex;
import com.dima_z.indexes.elastic.ElasticIndex;
import com.dima_z.indexes.mysql.MySQLIndex;
import com.dima_z.indexes.postgres.PostgresIndex;
import com.dima_z.indexes.suffixTree.SuffixTreeIndex;


public class IndexesFactory {
    
    public static IIndex createIndex(IndexType indexType) {
        if (indexType == IndexType.SUFFIX_TREE) {
            return new SuffixTreeIndex();
        } else if (indexType == IndexType.ELASTIC_TREE) {
            return new ElasticIndex();
        } else if (indexType == IndexType.MYSQL_TREE) {
            return new MySQLIndex();
        } else if (indexType == IndexType.POSTGRES_TREE) {
            return new PostgresIndex();
        } else if (indexType == IndexType.CLICKHOUSE_TREE) {
            return new ClickHouseIndex();
        }
        return new SuffixTreeIndex();
    }
}
