package com.dima_z.indexes;

import java.util.Collection;
import java.util.List;

import com.dima_z.pojo.IndexValueObject;

public interface IIndex {
    public Collection<Integer> search(String query);
    public Collection<Integer> includes(String query);
    public Collection<Integer> notIncludes(String query);
    public Collection<Integer> startsWith(String query);
    public Collection<Integer> notStartsWith(String query);
    public Collection<Integer> endsWith(String query);
    public Collection<Integer> notEndsWith(String query);
    public void createIndex();
    public void dropIndex();
    public void bulkInsert(List<IndexValueObject> items);
    public boolean isEmpty();
}
