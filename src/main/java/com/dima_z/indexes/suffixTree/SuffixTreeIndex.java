package com.dima_z.indexes.suffixTree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.abahgat.suffixtree.GeneralizedSuffixTree;
import com.dima_z.dataReader.CSVFileStreamer;
import com.dima_z.indexes.IIndex;
import com.dima_z.pojo.IndexValueObject;

public class SuffixTreeIndex implements IIndex {

    private GeneralizedSuffixTree treeInstance = null;
    private byte[] ids = null;
    private final int INITIAL_SIZE = 1024;

    public SuffixTreeIndex() {
        System.out.println("SuffixTreeIndex created");
        this.treeInstance = new GeneralizedSuffixTree();
    }

    public void put(String key, int value) {
        this.treeInstance.put(key, value);
        if (value >= this.ids.length * 0.75) {
            byte[] newIds = new byte[this.ids.length * 2];
            System.arraycopy(this.ids, 0, newIds, 0, this.ids.length);
            this.ids = newIds;
        }
        this.ids[value] = 1;
    }

    @Override
    public Collection<Integer> search(String query) {
        return this.treeInstance.searchWord(query);
    }

    @Override
    public Collection<Integer> includes(String query) {
        return this.treeInstance.search(query);
    }

    @Override
    public Collection<Integer> notIncludes(String query) {
        Collection<Integer> resultIncludes = this.treeInstance.search(query);
        Collection<Integer> resultNotIncludes = new ArrayList<Integer>();
        for (int i = 0; i < this.ids.length; i++) {
            if (this.ids[i] == 1 && !resultIncludes.contains(i)) {
                resultNotIncludes.add(i);
            }
        }
        return resultNotIncludes;
    }

    @Override
    public Collection<Integer> startsWith(String query) {
        return this.treeInstance.startsWith(query);
    }

    @Override
    public Collection<Integer> notStartsWith(String query) {
        Collection<Integer> resultStartsWith = this.treeInstance.startsWith(query);
        Collection<Integer> resultNotStartsWith = new ArrayList<Integer>();
        for (int i = 0; i < this.ids.length; i++) {
            if (this.ids[i] == 1 && !resultStartsWith.contains(i)) {
                resultNotStartsWith.add(i);
            }
        }
        return resultNotStartsWith;
    }

    @Override
    public Collection<Integer> endsWith(String query) {
        return this.treeInstance.endsWith(query);
    }

    @Override
    public Collection<Integer> notEndsWith(String query) {
        Collection<Integer> resultEndsWith = this.treeInstance.endsWith(query);
        Collection<Integer> resultNotEndsWith = new ArrayList<Integer>();
        for (int i = 0; i < this.ids.length; i++) {
            if (this.ids[i] == 1 && !resultEndsWith.contains(i)) {
                resultNotEndsWith.add(i);
            }
        }
        return resultNotEndsWith;
    }

    @Override
    public void bulkInsert(List<IndexValueObject> items) {
        for (IndexValueObject item : items) {
            this.put(item.name, Integer.parseInt(item.id));
        }
    }

    public boolean isEmpty() {
        return this.ids == null || this.ids.length == 0;
    }

    @Override
    public void dropIndex() {
        return;
    }

    @Override
    public void createIndex() {
        if (this.ids == null) {
            this.ids = new byte[INITIAL_SIZE];
        }
    }

    private long roundTo(long value, long round) {
        return (value + (round - 1)) & ~(round - 1);
    }

    public long getIndexSize() {
        int MB = 1024*1024;
        long indexSize = this.roundTo((16 + 4 + 4 + 4 + this.roundTo(this.ids.length, 4)), 8);
        long treeSize = this.treeInstance.getTreeMemorySize();

        return this.roundTo(indexSize + treeSize, 8) / MB;
    }

    public static void main(String[] args) {
        SuffixTreeIndex currentIndex = new SuffixTreeIndex();
        currentIndex.createIndex();
        int indexSize = 2000000;


        int currentIndexSize = 0;
        String sourceCSVFileName = "./data/geo_dataset_utf8_2M.csv";
        int batchSize = 50000;
        CSVFileStreamer csvStreamer = new CSVFileStreamer(sourceCSVFileName, batchSize);
        Iterator<List<IndexValueObject>> iterator = csvStreamer.iterator();
        List<IndexValueObject> currentBatch = null;
        
        while (iterator.hasNext()) {
                //System.out.println(">>>>>" + currentIndexSize + " <<<<<");
            currentBatch = iterator.next();
            currentIndex.bulkInsert(currentBatch);
            currentIndexSize += currentBatch.size();
            if (currentIndexSize >= indexSize) {
               break;
            }
        }

        System.out.println("IndexSize rows: " + currentIndexSize + " items");
        System.out.println("getIndexSize memory: " + currentIndex.getIndexSize() + " MB");
    }
}
