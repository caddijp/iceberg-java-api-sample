package com.caddi.icebergjavasample;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

public class JsonlReader implements Iterator<Map<String,Object>>, Closeable {

    private final ObjectMapper mapper =
            new ObjectMapper()
                    .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final TypeReference<Map<String, Object>> reference = new TypeReference<>() {};

    private final InputStream input;
    private final Iterator<String> iterator;

    public JsonlReader(InputStream input) throws IOException {
        this.input = input;
        this.iterator = new BufferedReader(new InputStreamReader(input)).lines().iterator();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Map<String, Object> next() {
        var line = this.iterator.next();
        try {
            return mapper.readValue(line, reference);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.input.close();
    }
}
