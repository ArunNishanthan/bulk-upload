package com.example.demo.ingestion.service;

import com.example.demo.ingestion.model.FileIngestionResult;
import com.example.demo.ingestion.model.IngestionJob;
import com.example.demo.ingestion.model.IngestionJobFile;
import com.example.demo.ingestion.model.IngestionJobStatus;
import com.example.demo.ingestion.model.UploadResponse;
import com.example.demo.ingestion.support.CamelCsvParserFactory;
import com.example.demo.ingestion.support.CompressionSupport;
import com.example.demo.ingestion.support.FileProcessingException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import lombok.extern.slf4j.Slf4j;

/**
 * Service responsible for ingesting account-to-product mappings from CSV
 * sources into MongoDB.
 * <p>
 * Supports both synchronous uploads (immediate ingestion) and asynchronous jobs
 * that can be
 * monitored via {@link IngestionJob} state persisted in MongoDB.
 */
@Slf4j
@Service
public class BulkUploadService {

    private static final String COLLECTION_NAME = "account_products";
    private static final String UNKNOWN_FILENAME = "unknown";
    private static final String[] EXPORT_HEADERS = { "accountNumber", "productCode" };

    private final MongoTemplate mongoTemplate;
    private final MongoCollection<Document> collection;
    private final ExecutorService executor;
    private final int batchSize;
    private final long progressUpdateInterval;
    private final AccountProductCsvIngestor csvIngestor;

    public BulkUploadService(MongoTemplate mongoTemplate,
            CamelCsvParserFactory parserFactory,
            CompressionSupport compressionSupport,
            ExecutorService executor,
            @Value("${app.ingestion.batch-size:5000}") int batchSize,
            @Value("${app.ingestion.progress-update-interval:50000}") long progressUpdateInterval) {
        this.mongoTemplate = mongoTemplate;
        this.collection = mongoTemplate.getCollection(COLLECTION_NAME);
        this.executor = executor;
        this.batchSize = batchSize;
        this.progressUpdateInterval = Math.max(1, progressUpdateInterval);
        BulkWriteOptions unordered = new BulkWriteOptions().ordered(false);
        this.csvIngestor = new AccountProductCsvIngestor(
                parserFactory,
                compressionSupport,
                collection,
                unordered,
                batchSize,
                this.progressUpdateInterval);
    }

    public UploadResponse ingest(MultipartFile[] files) {
        List<MultipartFile> validFiles = normalizeFiles(files);
        if (validFiles.isEmpty()) {
            return new UploadResponse(Collections.emptyList(), 0, 0, 0, 0, 0);
        }

        Instant overallStart = Instant.now();
        List<FileIngestionResult> perFileResults = new ArrayList<>(validFiles.size());

        for (MultipartFile file : validFiles) {
            perFileResults.add(processMultipartFile(file, null));
        }

        return toUploadResponse(perFileResults, overallStart);
    }

    private UploadResponse toUploadResponse(List<FileIngestionResult> perFileResults, Instant startedAt) {
        long totalRecords = 0;
        long totalInserted = 0;
        long totalDuplicates = 0;
        long totalInvalid = 0;

        for (FileIngestionResult result : perFileResults) {
            totalRecords += result.totalRecords();
            totalInserted += result.insertedRecords();
            totalDuplicates += result.duplicateRecords();
            totalInvalid += result.invalidRecords();
        }

        long durationMillis = Duration.between(startedAt, Instant.now()).toMillis();
        return new UploadResponse(perFileResults, totalRecords, totalInserted, totalDuplicates, totalInvalid,
                durationMillis);
    }

    public IngestionJob enqueue(MultipartFile[] files, boolean deleteExisting) {
        List<MultipartFile> validFiles = normalizeFiles(files);
        if (validFiles.isEmpty()) {
            throw new FileProcessingException("At least one file is required for ingestion");
        }
        if (jobInProgress()) {
            throw new FileProcessingException("An ingestion job is already running. Please wait for it to finish.");
        }

        List<TempFile> tempFiles = spoolFiles(validFiles);
        IngestionJob job = createPendingJob(tempFiles, deleteExisting);
        mongoTemplate.save(job);
        executor.submit(() -> executeJob(job.getId(), tempFiles));
        return job;
    }

    public IngestionJob findJob(String jobId) {
        return mongoTemplate.findById(jobId, IngestionJob.class);
    }

    public IngestionJob resetJob(String jobId) {
        IngestionJob job = mongoTemplate.findById(jobId, IngestionJob.class);
        if (job == null) {
            return null;
        }

        job.setStatus(IngestionJobStatus.PENDING);
        job.setCreatedAt(Instant.now());
        job.setStartedAt(null);
        job.setCompletedAt(null);
        job.setErrorMessage(null);
        resetJobCounters(job);
        job.setFiles(new ArrayList<>());
        job.setDeleteExisting(false);

        mongoTemplate.save(job);
        return job;
    }

    private IngestionJob createPendingJob(List<TempFile> tempFiles, boolean deleteExisting) {
        IngestionJob job = new IngestionJob();
        job.setId(UUID.randomUUID().toString());
        job.setStatus(IngestionJobStatus.PENDING);
        job.setCreatedAt(Instant.now());
        job.setDeleteExisting(deleteExisting);
        resetJobCounters(job);
        job.setFiles(tempFiles.stream()
                .map(temp -> IngestionJobFile.pending(temp.originalFilename()))
                .collect(Collectors.toCollection(ArrayList::new)));
        return job;
    }

    private void resetJobCounters(IngestionJob job) {
        job.setDeletedRecords(0);
        job.setTotalRecordsEstimate(-1);
        job.setProcessedRecords(0);
        job.setProgressPercent(0);
        job.setTotalRecords(0);
        job.setInsertedRecords(0);
        job.setDuplicateRecords(0);
        job.setInvalidRecords(0);
    }

    private boolean jobInProgress() {
        Query query = Query.query(Criteria.where("status").in(IngestionJobStatus.RUNNING, IngestionJobStatus.PENDING));
        if (mongoTemplate.exists(query, IngestionJob.class)) {
            log.warn("Rejecting ingestion request because another job is in progress");
            return true;
        }
        return false;
    }

    private FileIngestionResult processMultipartFile(MultipartFile multipartFile, ProgressListener listener) {
        String filename = resolveFilename(multipartFile.getOriginalFilename());
        try (InputStream rawStream = multipartFile.getInputStream()) {
            return csvIngestor.ingest(filename, rawStream, listener);
        } catch (IOException ex) {
            throw new FileProcessingException("Failed to read input stream for file %s".formatted(filename), ex);
        }
    }

    private FileIngestionResult processTempFile(TempFile tempFile, ProgressListener listener) {
        try (InputStream rawStream = Files.newInputStream(tempFile.path())) {
            return csvIngestor.ingest(tempFile.originalFilename(), rawStream, listener);
        } catch (IOException ex) {
            throw new FileProcessingException("Failed to read buffered file %s".formatted(tempFile.originalFilename()),
                    ex);
        }
    }

    private void executeJob(String jobId, List<TempFile> tempFiles) {
        IngestionJob job = mongoTemplate.findById(jobId, IngestionJob.class);
        if (job == null) {
            tempFiles.forEach(TempFile::deleteSilently);
            return;
        }

        prepareJobForExecution(job, calculateTotalEstimate(tempFiles));
        List<IngestionJobFile> fileStatuses = new ArrayList<>(tempFiles.size());
        try {
            if (job.isDeleteExisting()) {
                deleteExistingDocuments(jobId, job);
            }
            JobIngestionTotals totals = ingestFiles(job, tempFiles, fileStatuses);
            finalizeSuccessfulJob(job, totals, fileStatuses);
        } catch (Exception ex) {
            handleJobFailure(jobId, job, fileStatuses, ex);
        } finally {
            tempFiles.forEach(TempFile::deleteSilently);
        }
    }

    private void prepareJobForExecution(IngestionJob job, long totalEstimate) {
        job.setTotalRecordsEstimate(totalEstimate);
        job.setProcessedRecords(0);
        job.setProgressPercent(0);
        job.setTotalRecords(0);
        job.setInsertedRecords(0);
        job.setDuplicateRecords(0);
        job.setInvalidRecords(0);
        job.setStatus(IngestionJobStatus.RUNNING);
        job.setStartedAt(Instant.now());
        mongoTemplate.save(job);
    }

    private void deleteExistingDocuments(String jobId, IngestionJob job) {
        long start = System.nanoTime();
        long existingCount = collection.estimatedDocumentCount();
        boolean dropped = tryDropCollection();
        if (!dropped) {
            existingCount = collection.deleteMany(new Document()).getDeletedCount();
        }
        job.setDeletedRecords(existingCount);
        mongoTemplate.save(job);
        log.info("Job {} cleared existing documents using {} in {} ms",
                jobId,
                dropped ? "drop" : "deleteMany",
                Duration.ofNanos(System.nanoTime() - start).toMillis());
    }

    private JobIngestionTotals ingestFiles(IngestionJob job,
            List<TempFile> tempFiles,
            List<IngestionJobFile> fileStatuses) {
        long totalRecords = 0;
        long totalInserted = 0;
        long totalDuplicates = 0;
        long totalInvalid = 0;

        for (TempFile tempFile : tempFiles) {
            // Snapshot the current totals so progress updates remain monotonic within the
            // listener.
            long baseProcessed = totalRecords;
            long baseInserted = totalInserted;
            long baseDuplicates = totalDuplicates;
            long baseInvalid = totalInvalid;

            ProgressListener listener = (processed, inserted, duplicates, invalid) -> updateJobProgress(
                    job,
                    baseProcessed + processed,
                    baseInserted + inserted,
                    baseDuplicates + duplicates,
                    baseInvalid + invalid);

            FileIngestionResult result = processTempFile(tempFile, listener);
            fileStatuses.add(IngestionJobFile.completed(result));

            totalRecords += result.totalRecords();
            totalInserted += result.insertedRecords();
            totalDuplicates += result.duplicateRecords();
            totalInvalid += result.invalidRecords();

            updateJobProgress(job, totalRecords, totalInserted, totalDuplicates, totalInvalid);
        }

        return new JobIngestionTotals(totalRecords, totalInserted, totalDuplicates, totalInvalid);
    }

    private void finalizeSuccessfulJob(IngestionJob job,
            JobIngestionTotals totals,
            List<IngestionJobFile> fileStatuses) {
        job.setStatus(IngestionJobStatus.SUCCEEDED);
        job.setCompletedAt(Instant.now());
        job.setTotalRecords(totals.totalRecords());
        job.setInsertedRecords(totals.inserted());
        job.setDuplicateRecords(totals.duplicates());
        job.setInvalidRecords(totals.invalid());
        job.setProcessedRecords(totals.totalRecords());
        job.setProgressPercent(100);
        if (job.getTotalRecordsEstimate() < 0) {
            job.setTotalRecordsEstimate(totals.totalRecords());
        }
        job.setFiles(new ArrayList<>(fileStatuses));
        mongoTemplate.save(job);
    }

    private void handleJobFailure(String jobId,
            IngestionJob job,
            List<IngestionJobFile> fileStatuses,
            Exception ex) {
        log.error("Job {} failed: {}", jobId, ex.getMessage(), ex);
        job.setStatus(IngestionJobStatus.FAILED);
        job.setCompletedAt(Instant.now());
        job.setErrorMessage(ex.getMessage());
        job.setFiles(new ArrayList<>(fileStatuses));
        mongoTemplate.save(job);
    }

    private List<TempFile> spoolFiles(List<MultipartFile> files) {
        List<TempFile> tempFiles = new ArrayList<>();
        try {
            for (MultipartFile file : files) {
                Path tempPath = Files.createTempFile("ingestion-", ".upload");
                try (InputStream in = file.getInputStream()) {
                    Files.copy(in, tempPath, StandardCopyOption.REPLACE_EXISTING);
                }
                String originalName = resolveFilename(file.getOriginalFilename());
                if (UNKNOWN_FILENAME.equals(originalName)) {
                    originalName = tempPath.getFileName().toString();
                }
                tempFiles.add(new TempFile(originalName, tempPath));
            }
        } catch (IOException ex) {
            tempFiles.forEach(TempFile::deleteSilently);
            throw new FileProcessingException("Failed to buffer uploaded files for asynchronous processing", ex);
        }
        if (tempFiles.isEmpty()) {
            throw new FileProcessingException("No valid files were provided");
        }
        return tempFiles;
    }

    private List<MultipartFile> normalizeFiles(MultipartFile[] files) {
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(files)
                .filter(this::isNonEmptyFile)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private boolean isNonEmptyFile(MultipartFile file) {
        if (file == null) {
            log.warn("Skipping null file entry provided for ingestion");
            return false;
        }
        if (file.isEmpty()) {
            log.warn("Skipping empty file entry provided for ingestion: {}",
                    resolveFilename(file.getOriginalFilename()));
            return false;
        }
        return true;
    }

    private String resolveFilename(String originalFilename) {
        if (originalFilename == null) {
            return UNKNOWN_FILENAME;
        }
        String trimmed = originalFilename.trim();
        return trimmed.isEmpty() ? UNKNOWN_FILENAME : trimmed;
    }

    private boolean tryDropCollection() {
        try {
            collection.drop();
            // Ensure the collection handle is valid for subsequent writes
            mongoTemplate.getCollection(collection.getNamespace().getCollectionName());
            return true;
        } catch (Exception ex) {
            log.warn("Collection drop failed, falling back to deleteMany: {}", ex.getMessage());
            return false;
        }
    }

    private long calculateTotalEstimate(List<TempFile> tempFiles) {
        long total = 0;
        boolean hasEstimate = false;
        for (TempFile tempFile : tempFiles) {
            long estimate = estimateTotalRecords(tempFile);
            if (estimate > 0) {
                total += estimate;
                hasEstimate = true;
            }
        }
        return hasEstimate ? total : -1;
    }

    private long estimateTotalRecords(TempFile tempFile) {
        return csvIngestor.estimateRecordCount(tempFile.path(), tempFile.originalFilename());
    }

    private void updateJobProgress(IngestionJob job,
            long processed,
            long inserted,
            long duplicates,
            long invalid) {
        boolean changed = false;
        if (job.getProcessedRecords() != processed) {
            job.setProcessedRecords(processed);
            changed = true;
        }
        if (job.getInsertedRecords() != inserted) {
            job.setInsertedRecords(inserted);
            changed = true;
        }
        if (job.getDuplicateRecords() != duplicates) {
            job.setDuplicateRecords(duplicates);
            changed = true;
        }
        if (job.getInvalidRecords() != invalid) {
            job.setInvalidRecords(invalid);
            changed = true;
        }
        if (!changed) {
            return;
        }

        job.setTotalRecords(processed);
        long estimate = job.getTotalRecordsEstimate();
        if (estimate > 0) {
            int percent = (int) Math.min(100, Math.round((processed * 100.0) / estimate));
            job.setProgressPercent(percent);
        }
        mongoTemplate.save(job);
    }

    public void exportAccountProducts(OutputStream outputStream) {
        CsvWriterSettings settings = new CsvWriterSettings();
        settings.setNullValue("");

        try (OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
                CsvWriter csvWriter = new CsvWriter(writer, settings);
                MongoCursor<Document> cursor = collection.find().batchSize(batchSize).iterator()) {

            csvWriter.writeHeaders(EXPORT_HEADERS);
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                csvWriter.writeRow(doc.getString("accountNumber"), doc.getString("productCode"));
            }
            csvWriter.flush();
        } catch (IOException ex) {
            throw new FileProcessingException("Failed to export account_products", ex);
        }
    }

    private record TempFile(String originalFilename, Path path) {
        private void deleteSilently() {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ex) {
                log.warn("Failed to delete temp file {}: {}", path, ex.getMessage());
            }
        }
    }

    private record JobIngestionTotals(long totalRecords, long inserted, long duplicates, long invalid) {
    }
}
