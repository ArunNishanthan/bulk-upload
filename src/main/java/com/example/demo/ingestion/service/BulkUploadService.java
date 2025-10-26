package com.example.demo.ingestion.service;

import com.example.demo.ingestion.model.AccountProductDocument;
import com.example.demo.ingestion.model.FileIngestionResult;
import com.example.demo.ingestion.model.IngestionJob;
import com.example.demo.ingestion.model.IngestionJobFile;
import com.example.demo.ingestion.model.IngestionJobStatus;
import com.example.demo.ingestion.support.CamelCsvParserFactory;
import com.example.demo.ingestion.support.CompressionSupport;
import com.example.demo.ingestion.support.FileProcessingException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Slf4j
@Service
public class BulkUploadService {

    private static final String COLLECTION_NAME = "account_products";
    private static final String UNKNOWN_FILENAME = "unknown";
    private static final int DUPLICATE_KEY_ERROR_CODE = 11000;
    private static final String[] EXPORT_HEADERS = { "accountNumber", "productCode" };

    private final MongoTemplate mongoTemplate;
    private final MongoCollection<Document> collection;
    private final CamelCsvParserFactory parserFactory;
    private final CompressionSupport compressionSupport;
    private final ExecutorService executor;
    private final BulkWriteOptions writeOptions;
    private final int batchSize;

    public BulkUploadService(MongoTemplate mongoTemplate,
            CamelCsvParserFactory parserFactory,
            CompressionSupport compressionSupport,
            ExecutorService executor,
            @Value("${app.ingestion.batch-size:5000}") int batchSize) {
        this.mongoTemplate = mongoTemplate;
        this.collection = mongoTemplate.getCollection(COLLECTION_NAME);
        this.parserFactory = parserFactory;
        this.compressionSupport = compressionSupport;
        this.executor = executor;
        this.writeOptions = new BulkWriteOptions().ordered(false);
        this.batchSize = Math.max(1, batchSize);
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

    public boolean deleteJob(String jobId) {
        IngestionJob job = mongoTemplate.findById(jobId, IngestionJob.class);
        if (job == null) {
            return false;
        }
        if (job.getStatus() == IngestionJobStatus.RUNNING) {
            throw new FileProcessingException("Cannot delete a job while it is running");
        }
        mongoTemplate.remove(job);
        return true;
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

    private void executeJob(String jobId, List<TempFile> tempFiles) {
        IngestionJob job = mongoTemplate.findById(jobId, IngestionJob.class);
        if (job == null) {
            tempFiles.forEach(TempFile::deleteSilently);
            return;
        }

        job.setStatus(IngestionJobStatus.RUNNING);
        job.setStartedAt(Instant.now());
        job.setErrorMessage(null);
        mongoTemplate.save(job);

        List<IngestionJobFile> fileStatuses = new ArrayList<>(job.getFiles());
        long totalRecords = 0;
        long insertedRecords = 0;
        long duplicateRecords = 0;
        long invalidRecords = 0;
        int activeIndex = -1;

        try {
            if (job.isDeleteExisting()) {
                long deleted = deleteExistingDocuments(jobId);
                job.setDeletedRecords(deleted);
                mongoTemplate.save(job);
            }

            for (int i = 0; i < tempFiles.size(); i++) {
                activeIndex = i;
                TempFile tempFile = tempFiles.get(i);
                FileIngestionResult result = ingestTempFile(tempFile);
                fileStatuses.set(i, IngestionJobFile.completed(result));

                totalRecords += result.totalRecords();
                insertedRecords += result.insertedRecords();
                duplicateRecords += result.duplicateRecords();
                invalidRecords += result.invalidRecords();
            }

            job.setStatus(IngestionJobStatus.SUCCEEDED);
            job.setCompletedAt(Instant.now());
            job.setTotalRecords(totalRecords);
            job.setInsertedRecords(insertedRecords);
            job.setDuplicateRecords(duplicateRecords);
            job.setInvalidRecords(invalidRecords);
            job.setFiles(fileStatuses);
            mongoTemplate.save(job);
        } catch (Exception ex) {
            log.error("Job {} failed: {}", jobId, ex.getMessage(), ex);
            job.setStatus(IngestionJobStatus.FAILED);
            job.setCompletedAt(Instant.now());
            job.setErrorMessage(ex.getMessage());

            if (activeIndex >= 0 && activeIndex < fileStatuses.size()) {
                TempFile failedFile = tempFiles.get(activeIndex);
                fileStatuses.set(activeIndex, IngestionJobFile.failed(failedFile.originalFilename(), ex.getMessage()));
            }
            job.setFiles(fileStatuses);
            mongoTemplate.save(job);
        } finally {
            tempFiles.forEach(TempFile::deleteSilently);
        }
    }

    private long deleteExistingDocuments(String jobId) {
        long start = System.nanoTime();
        long existingCount = collection.estimatedDocumentCount();
        boolean dropped = tryDropCollection();
        if (!dropped) {
            existingCount = collection.deleteMany(new Document()).getDeletedCount();
        }
        log.info("Job {} cleared {} existing documents using {} in {} ms",
                jobId,
                existingCount,
                dropped ? "drop" : "deleteMany",
                Duration.ofNanos(System.nanoTime() - start).toMillis());
        return existingCount;
    }

    private FileIngestionResult ingestTempFile(TempFile tempFile) {
        try (InputStream rawStream = Files.newInputStream(tempFile.path())) {
            return ingestStream(tempFile.originalFilename(), rawStream);
        } catch (IOException ex) {
            throw new FileProcessingException("Failed to read buffered file %s".formatted(tempFile.originalFilename()),
                    ex);
        }
    }

    private FileIngestionResult ingestStream(String filename, InputStream rawStream) {
        Instant start = Instant.now();
        long processed = 0;
        long inserted = 0;
        long duplicates = 0;
        long invalid = 0;
        CsvParser parser = parserFactory.newParser();
        List<WriteModel<Document>> batch = new ArrayList<>(batchSize);

        try (InputStream decoded = compressionSupport.decodeIfNecessary(rawStream, filename);
                Reader reader = new InputStreamReader(new BufferedInputStream(decoded), StandardCharsets.UTF_8)) {
            parser.beginParsing(reader);
            boolean headerSkipped = false;
            String[] row;
            while ((row = parser.parseNext()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }

                processed++;
                Document doc = toDocument(row);
                if (doc == null) {
                    invalid++;
                    continue;
                }

                batch.add(new InsertOneModel<>(doc));
                if (batch.size() >= batchSize) {
                    BulkWriteSummary summary = writeBatch(batch);
                    inserted += summary.inserted();
                    duplicates += summary.duplicates();
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                BulkWriteSummary summary = writeBatch(batch);
                inserted += summary.inserted();
                duplicates += summary.duplicates();
                batch.clear();
            }
        } catch (IOException ex) {
            throw new FileProcessingException("Failed to process CSV for file %s".formatted(filename), ex);
        } finally {
            parser.stopParsing();
        }

        long durationMillis = Duration.between(start, Instant.now()).toMillis();
        return new FileIngestionResult(filename, processed, inserted, duplicates, invalid, durationMillis);
    }

    private Document toDocument(String[] row) {
        if (row == null || row.length < 2) {
            return null;
        }
        String accountNumber = safeTrim(row[0]);
        String productCode = safeTrim(row[1]);
        if (!isValidAccountNumber(accountNumber) || !isValidProductCode(productCode)) {
            return null;
        }

        Document doc = new Document();
        doc.put("_id", AccountProductDocument.toId(accountNumber, productCode));
        doc.put("accountNumber", accountNumber);
        doc.put("productCode", productCode);
        return doc;
    }

    private static String safeTrim(String value) {
        return value == null ? "" : value.trim();
    }

    private static boolean isValidAccountNumber(String accountNumber) {
        return !accountNumber.isEmpty() && accountNumber.length() <= 15;
    }

    private static boolean isValidProductCode(String productCode) {
        return !productCode.isEmpty() && productCode.length() <= 4;
    }

    private BulkWriteSummary writeBatch(List<WriteModel<Document>> batch) {
        if (batch.isEmpty()) {
            return BulkWriteSummary.EMPTY;
        }
        try {
            BulkWriteResult result = collection.bulkWrite(batch, writeOptions);
            return new BulkWriteSummary(result.getInsertedCount(), 0);
        } catch (MongoBulkWriteException ex) {
            long duplicates = ex.getWriteErrors().stream()
                    .filter(error -> error.getCode() == DUPLICATE_KEY_ERROR_CODE)
                    .count();
            long inserted = ex.getWriteResult() != null ? ex.getWriteResult().getInsertedCount() : 0;
            long otherErrors = ex.getWriteErrors().stream()
                    .map(BulkWriteError::getCode)
                    .filter(code -> code != DUPLICATE_KEY_ERROR_CODE)
                    .count();

            if (otherErrors > 0) {
                throw new FileProcessingException("Bulk write failed with non-duplicate errors", ex);
            }
            return new BulkWriteSummary(inserted, duplicates);
        }
    }

    private IngestionJob createPendingJob(List<TempFile> tempFiles, boolean deleteExisting) {
        IngestionJob job = new IngestionJob();
        job.setId(UUID.randomUUID().toString());
        job.setStatus(IngestionJobStatus.PENDING);
        job.setCreatedAt(Instant.now());
        job.setDeleteExisting(deleteExisting);
        job.setDeletedRecords(0);
        job.setTotalRecords(0);
        job.setInsertedRecords(0);
        job.setDuplicateRecords(0);
        job.setInvalidRecords(0);
        job.setFiles(tempFiles.stream()
                .map(temp -> IngestionJobFile.pending(temp.originalFilename()))
                .collect(Collectors.toCollection(ArrayList::new)));
        return job;
    }

    private boolean jobInProgress() {
        Query query = Query.query(Criteria.where("status").in(IngestionJobStatus.RUNNING, IngestionJobStatus.PENDING));
        if (mongoTemplate.exists(query, IngestionJob.class)) {
            log.warn("Rejecting ingestion request because another job is in progress");
            return true;
        }
        return false;
    }

    private List<MultipartFile> normalizeFiles(MultipartFile[] files) {
        if (files == null || files.length == 0) {
            return List.of();
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
            mongoTemplate.getCollection(collection.getNamespace().getCollectionName());
            return true;
        } catch (Exception ex) {
            log.warn("Collection drop failed, falling back to deleteMany: {}", ex.getMessage());
            return false;
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

    private record BulkWriteSummary(long inserted, long duplicates) {
        private static final BulkWriteSummary EMPTY = new BulkWriteSummary(0, 0);
    }
}
