package com.dima_z.indexes;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import java.util.Iterator;

import com.dima_z.dataReader.CSVFileStreamer;
import com.dima_z.dataWriter.DataWriter;
import com.dima_z.indexes.indexFactory.IndexType;
import com.dima_z.indexes.indexFactory.IndexesFactory;
import com.dima_z.indexes.testCases.QueryType;
import com.dima_z.indexes.testCases.TestCasesFactory;
import com.dima_z.pojo.IndexCreateValueObject;
import com.dima_z.pojo.IndexValueObject;
import com.dima_z.pojo.TestResultObject;
import com.dima_z.utils.IWritable;

public class IndexesTest {

    public static IIndex currentIndex = null;
    public final static IndexType currentIndexType = IndexesTest.indexType(System.getProperty("indexType"));
    public final static int indexSize = IndexesTest.indexSize(System.getProperty("indexSize"));
    public final static int batchSize = 50000;

    private final static String sourceCSVFileName = "./data/geo_dataset_utf8_2M.csv";
    private final static String resultsCSVFileName = "./results/" + IndexesTest.currentIndexType.getType() + "/experimentStats" + indexSize / 1000 + "k.csv";
    private final static String resultsRawFileName = "./results/" + IndexesTest.currentIndexType.getType() + "/experimentStats" + indexSize / 1000 + "k.txt";
    private final static String creationStatsFileName = "./results/" + IndexesTest.currentIndexType.getType() + "/creationStats" + indexSize / 1000 + "k.csv";

    private List<IWritable> testStats = new ArrayList<IWritable>();
    private List<IWritable> creationStats = new ArrayList<IWritable>();

    private static IndexType indexType(String indexType) {
        System.out.println(">>>>>>indexType " + indexType);
        return IndexType.valueOf(indexType);
    }

    private static int indexSize(String indexSize) {
        System.out.println(">>>>>>indexSize " + indexSize);
        return Integer.parseInt(indexSize);
    }

    @BeforeAll
    public static void setup() {
        System.out.println("setup " + IndexesTest.currentIndexType.getType() + " started " + indexSize);
        currentIndex = IndexesFactory.createIndex(IndexesTest.currentIndexType);
    }

    @BeforeEach
    public void beforeEach() {
        System.out.println("before each");
        if (currentIndex.isEmpty()) {
            currentIndex.createIndex();
            
            int currentIndexSize = 0;
            CSVFileStreamer csvStreamer = new CSVFileStreamer(sourceCSVFileName, batchSize);
            Iterator<List<IndexValueObject>> iterator = csvStreamer.iterator();
            List<IndexValueObject> currentBatch = null;
            long startTimeAll = System.nanoTime();
            long startTime = 0;
            long endTime = 0;

            while (iterator.hasNext()) {
                //System.out.println(">>>>>" + currentIndexSize + " <<<<<");
                currentBatch = iterator.next();
                startTime = System.nanoTime();
                currentIndex.bulkInsert(currentBatch);
                endTime = System.nanoTime();
                creationStats.add(new IndexCreateValueObject(IndexesTest.currentIndexType.getType(), indexSize, endTime - startTime, currentIndexSize, currentIndexSize + currentBatch.size()));
                currentIndexSize += currentBatch.size();
                if (currentIndexSize >= indexSize) {
                    break;
                }
            }
            creationStats.add(new IndexCreateValueObject(IndexesTest.currentIndexType.getType(), currentIndexSize, endTime - startTimeAll, 0, currentIndexSize));
        }
    }

    @AfterEach
    public void afterEach() {
        System.out.println("after each");
        DataWriter dataWriter = DataWriter.getInstance();
        dataWriter.writeCSVData(resultsCSVFileName, testStats);
        dataWriter.writeRawData(resultsRawFileName, testStats);
        dataWriter.writeCSVData(creationStatsFileName, creationStats);
        testStats = new ArrayList<>();
        creationStats = new ArrayList<>();
        currentIndex.dropIndex();
    }

    @AfterAll
    public static void finish() {
        System.out.println("finish");
        if (IndexesTest.currentIndex != null && !IndexesTest.currentIndex.isEmpty()) {
            IndexesTest.currentIndex.dropIndex();
        }
    }

    @Test
    public void testStartsQuery() {
        System.out.println("testStartsQuery");
        QueryType type = QueryType.STARTS_WITH;//"startsWithQuery";
        HashMap<String, String[]> startsWithQueries = TestCasesFactory.getTestCases(type);
        long startTime = 0;
        long endTime = 0;
        for (String key : startsWithQueries.keySet()) {
            String[] queries = startsWithQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.startsWith(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }

    @Test
    public void testNotStartsQuery() {
        System.out.println("testNotStartsQuery");
        QueryType type = QueryType.NOT_STARTS_WITH;//"notStartsQuery";
        HashMap<String, String[]> notStartsWithQueries = TestCasesFactory.getTestCases(QueryType.STARTS_WITH);
        long startTime = 0;
        long endTime = 0;
        for (String key : notStartsWithQueries.keySet()) {
            String[] queries = notStartsWithQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.notStartsWith(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }
    
    @Test
    public void testIncludesQuery() {
        System.out.println("testIncludesQuery");
        QueryType type = QueryType.INCLUDES;//"includesQuery";
        HashMap<String, String[]> includesQueries = TestCasesFactory.getTestCases(type);
        long startTime = 0;
        long endTime = 0;
        for (String key : includesQueries.keySet()) {
            String[] queries = includesQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.includes(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }

    @Test
    public void testNotIncludesQuery() {
        System.out.println("testNotIncludesQuery");
        QueryType type = QueryType.NOT_INCLUDES; //"notIncludesQuery";
        HashMap<String, String[]> includesQueries = TestCasesFactory.getTestCases(QueryType.INCLUDES);
        long startTime = 0;
        long endTime = 0;
        for (String key : includesQueries.keySet()) {
            String[] queries = includesQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.notIncludes(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }

    @Test
    public void testSearchQuery() {
        System.out.println("testSearchQuery");
        QueryType type = QueryType.SEARCH;//"searchQuery";
        HashMap<String, String[]> searchQueries = TestCasesFactory.getTestCases(type);
        long startTime = 0;
        long endTime = 0;
        for (String key : searchQueries.keySet()) {
            String[] queries = searchQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.search(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }

    @Test
    public void testEndsQuery() {
        System.out.println("testEndsQuery");
        QueryType type = QueryType.ENDS_WITH;//"endsWithQuery";
        HashMap<String, String[]> endsWithQueries = TestCasesFactory.getTestCases(type);
        long startTime = 0;
        long endTime = 0;
        for (String key : endsWithQueries.keySet()) {
            String[] queries = endsWithQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.endsWith(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }

    @Test
    public void testNotEndsQuery() {
        System.out.println("testNotEndsQuery");
        QueryType type = QueryType.NOT_ENDS_WITH;//"notEndsQuery";
        HashMap<String, String[]> endsWithQueries = TestCasesFactory.getTestCases(QueryType.ENDS_WITH);
        long startTime = 0;
        long endTime = 0;
        for (String key : endsWithQueries.keySet()) {
            String[] queries = endsWithQueries.get(key);
            for (String query : queries) {
                startTime = System.nanoTime();
                Collection<Integer> result = currentIndex.notEndsWith(query);
                endTime = System.nanoTime();
                testStats.add(new TestResultObject(IndexesTest.currentIndexType.getType(), type.getType(), key, query, endTime - startTime, result.size(), indexSize));
            }
        }
        assertEquals(0, 0);
    }
}