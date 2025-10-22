package com.eyelevel.documentprocessor.common.json.jackson;


import com.eyelevel.documentprocessor.common.json.JsonSerializer;
import com.eyelevel.documentprocessor.exception.json.JsonParsingException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component("jacksonJsonSerializer")
@RequiredArgsConstructor
@Slf4j
public class JacksonJsonSerializer implements JsonSerializer {

    private final ObjectMapper objectMapper;

    /**
     * Serializes a Java object into its JSON representation, defaulting to not pretty printing.
     *
     * @param object The Java object to serialize.
     * @param <T>    The type of the Java object.
     *
     * @return The JSON representation of the object as a string.
     *
     * @throws JsonParsingException if an error occurs during JSON
     *                              serialization.
     */
    @Override
    public <T> String serialize(T object) {
        return serialize(object, false); // Default to not pretty printing
    }

    /**
     * Serializes a Java object into its JSON representation, optionally with pretty printing (beautified).
     *
     * @param object      The Java object to serialize.
     * @param prettyPrint Whether to format the JSON with indentation and line breaks for readability.
     * @param <T>         The type of the Java object.
     *
     * @return The JSON representation of the object as a string.
     *
     * @throws JsonParsingException if an error occurs during JSON serialization.
     */
    @Override
    public <T> String serialize(T object, boolean prettyPrint) {
        log.debug("Serializing Java object to JSON string (prettyPrint: {}): {}", prettyPrint,
                  object.getClass().getName());
        try {
            String json;
            if (prettyPrint) {
                json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
            } else {
                json = objectMapper.writeValueAsString(object);
            }
            log.trace("Java object has been serialized to JSON: {}", json);
            return json;
        } catch (JsonProcessingException e) {
            log.error("Error serializing Java object to JSON", e);
            throw new JsonParsingException("Error serializing Java object to JSON", e);
        }
    }
}