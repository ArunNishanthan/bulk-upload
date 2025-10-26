package com.example.demo.ingestion.support;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CompressionSupport {

    private static final int SIGNATURE_LENGTH = 6;
    private static final int BUFFER_SIZE = 64 * 1024;

    public InputStream decodeIfNecessary(InputStream original, String filename) throws IOException {
        BufferedInputStream buffered = new BufferedInputStream(original, BUFFER_SIZE);
        if (isGzipStream(buffered) || isGzipFilename(filename)) {
            log.debug("Detected gzip stream for file={}", filename);
            return new GzipCompressorInputStream(buffered, true);
        }

        log.debug("Streaming file={} without decompression", filename);
        return buffered;
    }

    private static boolean isGzipStream(BufferedInputStream stream) throws IOException {
        stream.mark(SIGNATURE_LENGTH);
        byte[] signature = stream.readNBytes(SIGNATURE_LENGTH);
        stream.reset();
        return GzipCompressorInputStream.matches(signature, signature.length);
    }

    private static boolean isGzipFilename(String filename) {
        return filename != null && filename.toLowerCase(Locale.ROOT).endsWith(".gz");
    }
}
