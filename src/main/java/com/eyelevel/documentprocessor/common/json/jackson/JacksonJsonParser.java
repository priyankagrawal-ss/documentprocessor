package com.eyelevel.documentprocessor.common.json.jackson;


import com.eyelevel.documentprocessor.common.json.JsonParser;
import com.eyelevel.documentprocessor.exception.json.JsonParsingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of the {@link JsonParser} interface using the Jackson library.
 *
 * <p>This class provides methods for parsing JSON data into Java objects, lists, and other data
 * structures. It leverages the {@link ObjectMapper} from the Jackson library to perform the JSON
 * parsing.
 */
@Component("jacksonJsonParser")
@RequiredArgsConstructor
@Slf4j
public class JacksonJsonParser implements JsonParser {

    private final ObjectMapper objectMapper;

    /**
     * Parses JSON data from a string into a Java object of the specified type.
     *
     * @param json      The JSON data as a string.
     * @param valueType The class of the Java object to parse the JSON into.
     * @param <T>       The type of the Java object.
     *
     * @return The parsed Java object.
     *
     * @throws JsonParsingException if an error occurs during JSON parsing.
     */
    @Override
    public <T> T parseObject(String json, Class<T> valueType) {
        log.debug("Parsing JSON string to object of type: {}", valueType.getName());
        try {
            byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
            T result = parseJson(jsonBytes, valueType);
            log.trace("Parsing JSON successful: {}", result);
            return result;
        } catch (Exception e) {
            log.error("Error parsing JSON string to object of type: {}", valueType.getName(), e);
            throw new JsonParsingException("Error parsing JSON string", e);
        }
    }

    /**
     * Parses JSON data from a byte array into a Java object of the specified type.
     *
     * @param jsonBytes The JSON data as a byte array.
     * @param valueType The class of the Java object to parse the JSON into.
     * @param <T>       The type of the Java object.
     *
     * @return The parsed Java object.
     *
     * @throws JsonParsingException if an error occurs during JSON parsing.
     */
    @Override
    public <T> T parseObject(byte[] jsonBytes, Class<T> valueType) {
        log.debug("Parsing JSON byte array to object of type: {}", valueType.getName());
        try {
            T result = parseJson(jsonBytes, valueType);
            log.trace("Parsing JSON successful: {}", result);
            return result;
        } catch (Exception e) {
            log.error("Error parsing JSON byte array to object of type: {}", valueType.getName(), e);
            throw new JsonParsingException("Error parsing JSON byte array", e);
        }
    }

    /**
     * Generic method to handle all JSON parsing using a Class.
     *
     * @param jsonBytes The JSON data as a byte array.
     * @param valueType The class of the Java object to parse the JSON into.
     * @param <T>       The type of the Java object.
     *
     * @return The parsed Java object.
     *
     * @throws JsonParsingException if an error occurs during JSON parsing.
     */
    private <T> T parseJson(byte[] jsonBytes, Class<T> valueType) {
        log.trace("Parsing JSON with Class: {}", valueType.getName());
        try {
            T result = objectMapper.readValue(jsonBytes, valueType);
            log.trace("parsing was successful: {}", result);
            return result;
        } catch (IOException e) {
            log.error("Error parsing JSON with Class: {}", valueType.getName(), e);
            throw new JsonParsingException("Error parsing JSON with Class", e);
        }
    }


}