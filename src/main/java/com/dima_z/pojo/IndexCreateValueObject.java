package com.dima_z.pojo;

import com.dima_z.utils.IWritable;

public class IndexCreateValueObject implements IWritable {
    
    private String indexType;
    private int indexSize;
    private long insertionTime;
    private int chunkStart;
    private int chunkEnd;
    
    public IndexCreateValueObject(String indexType, int indexSize, long insertionTime, int chunkStart, int chunkEnd) {
        this.indexType = indexType;
        this.indexSize = indexSize;
        this.insertionTime = insertionTime;
        this.chunkStart = chunkStart;
        this.chunkEnd = chunkEnd;
    }

    public String getIndexType() {
        return indexType;
    }
    
    public int getIndexSize() {
        return indexSize;
    }
    
    public long getInsertionTime() {
        return insertionTime;
    }

    public int getChunkStart() {
        return chunkStart;
    }

    public int getChunkEnd() {
        return chunkEnd;
    }
    
    @Override
    public String toString() {
        return ">>>>" +
                "indexType=" + indexType +
                ", indexSize=" + indexSize +
                ", insertionTime=" + insertionTime + " ns" +
                ", chunkStart=" + chunkStart +
                ", chunkEnd=" + chunkEnd;
    }

    public String toCSV() {
        return indexType + "," + indexSize + "," + insertionTime + "," + chunkStart + "," + chunkEnd + "\n";
    }
}
