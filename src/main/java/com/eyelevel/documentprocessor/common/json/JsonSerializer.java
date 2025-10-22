package com.eyelevel.documentprocessor.common.json;

/**
 * Defines the contract for serializing Java objects into JSON data.
 *
 * <p>This interface provides methods for serializing a Java object into its JSON representation.
 * Implementations of this interface should handle the details of JSON serialization using a
 * specific JSON library (e.g., Jackson, Gson).
 */
public interface JsonSerializer {

    /**
     * Serializes a Java object into its JSON representation.
     *
     * @param object The Java object to serialize.
     * @param <T>    The type of the Java object.
     *
     * @return The JSON representation of the object as a string.
     *
     * @throws com.eyelevel.fraudx.exception.JsonParsingException if an error occurs during JSON
     *                                                            serialization.
     */
    <T> String serialize(T object);

    /**
     * Serializes a Java object into its JSON representation.
     *
     * @param object      The Java object to serialize.
     * @param prettyPrint whether to format the JSON with indentation and line breaks.
     * @param <T>         The type of the Java object.
     *
     * @return The JSON representation of the object as a string.
     *
     * @throws com.eyelevel.fraudx.exception.JsonParsingException if an error occurs during JSON
     *                                                            serialization.
     */
    <T> String serialize(T object, boolean prettyPrint);
}