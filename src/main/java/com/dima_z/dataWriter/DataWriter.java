package com.dima_z.dataWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import com.dima_z.utils.IWritable;

public class DataWriter {
        
    private DataWriter() {
        System.out.println("DataWriter");
    }

    private static DataWriter instance = null;

    public static DataWriter getInstance() {
        if (instance == null) {
            instance = new DataWriter();
        }
        return instance;
    }
    
    public void writeCSVData(String fileName, List<IWritable> data) {
        System.out.println("writeData");
        try {
            File file = new File(fileName);
            // Ensure parent directories exist
            File parentDir = file.getParentFile();
            if (!parentDir.exists()) {
                parentDir.mkdirs(); // Create parent directories if they do not exist
            }
            // Check if the file does not exist and create it
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
            for (int i = 0; i < data.size(); i++) {
                if (data.get(i) != null) {
                    bufferedWriter.write(data.get(i).toCSV());
                } else {
                    break;
                }
            }
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeRawData(String fileName, List<IWritable> data) {
        System.out.println("writeData");
        try {
            File file = new File(fileName);
            // Ensure parent directories exist
            File parentDir = file.getParentFile();
            if (!parentDir.exists()) {
                parentDir.mkdirs(); // Create parent directories if they do not exist
            }
            // Check if the file does not exist and create it
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
            for (int i = 0; i < data.size(); i++) {
                if (data.get(i) != null) {
                    bufferedWriter.write(data.get(i).toString());
                } else {
                    break;
                }
            }
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
