package com.example.demo.ingestion.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@ToString
@NoArgsConstructor
@Document(collection = "ingestion_jobs")
public class IngestionJob {

    @Id
    private String id;
    private IngestionJobStatus status;
    private Instant createdAt;
    private Instant startedAt;
    private Instant completedAt;
    private long totalRecords;
    private long insertedRecords;
    private long duplicateRecords;
    private long invalidRecords;
    private long processedRecords;
    private long totalRecordsEstimate;
    private int progressPercent;
    private boolean deleteExisting;
    private long deletedRecords;
    private String errorMessage;
    private List<IngestionJobFile> files = new ArrayList<>();
}
