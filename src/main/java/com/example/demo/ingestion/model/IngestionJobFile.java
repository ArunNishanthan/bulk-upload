package com.example.demo.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IngestionJobFile {

    private String filename;
    private IngestionJobStatus status;
    private long totalRecords;
    private long insertedRecords;
    private long duplicateRecords;
    private long invalidRecords;
    private long durationMillis;
    private String errorMessage;

    public static IngestionJobFile pending(String filename) {
        return new IngestionJobFile(filename, IngestionJobStatus.PENDING, 0, 0, 0, 0, 0, null);
    }

    public static IngestionJobFile completed(FileIngestionResult result) {
        return new IngestionJobFile(
            result.filename(),
            IngestionJobStatus.SUCCEEDED,
            result.totalRecords(),
            result.insertedRecords(),
            result.duplicateRecords(),
            result.invalidRecords(),
            result.durationMillis(),
            null
        );
    }

}
