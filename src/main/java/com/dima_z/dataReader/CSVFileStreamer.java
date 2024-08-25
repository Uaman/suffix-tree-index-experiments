package com.dima_z.dataReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.dima_z.pojo.IndexValueObject;

public class CSVFileStreamer implements Iterable {
    
    private String filePath;
    private int batchSize;

    public CSVFileStreamer(String filePath, int batchSize) {
        this.filePath = filePath;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<List<IndexValueObject>> iterator() {
        return new Iterator<List<IndexValueObject>>() {
            private BufferedReader reader;
            private String nextLine;

            {
                try {
                    reader = new BufferedReader(new FileReader(filePath));
                    nextLine = reader.readLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return nextLine != null;
            }

            @Override
            public List<IndexValueObject> next() {
                if (nextLine == null) {
                    throw new NoSuchElementException();
                }
                int i = 0;
                List<IndexValueObject> result = new ArrayList<>();
                try {
                    IndexValueObject indexValueObject = null;
                    while (i < batchSize && nextLine != null) {
                        indexValueObject = new IndexValueObject(nextLine.split(","));
                        result.add(indexValueObject);
                        nextLine = reader.readLine();
                        i++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return result;
            }

            public void close() {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }
}
