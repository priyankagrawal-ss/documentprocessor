package com.eyelevel.documentprocessor.common.json;

/**
 * Defines the contract for parsing JSON data.
 *
 * <p>This interface provides methods for parsing JSON data into Java objects, lists, and other data
 * structures. Implementations of this interface should handle the details of JSON parsing using a
 * specific JSON library (e.g., Jackson, Gson).
 */
public interface JsonParser {

    /**
     * Parses JSON data from a string into a Java object of the specified type.
     *
     * @param json      The JSON data as a string.
     * @param valueType The class of the Java object to parse the JSON into.
     * @param <T>       The type of the Java object.
     *
     * @return The parsed Java object.
     *
     * @throws com.eyelevel.fraudx.exception.JsonParsingException if an error occurs during JSON
     *                                                            parsing.
     */
    <T> T parseObject(String json, Class<T> valueType);

    /**
     * Parses JSON data from a byte array into a Java object of the specified type.
     *
     * @param jsonBytes The JSON data as a byte array.
     * @param valueType The class of the Java object to parse the JSON into.
     * @param <T>       The type of the Java object.
     *
     * @return The parsed Java object.
     *
     * @throws com.eyelevel.fraudx.exception.JsonParsingException if an error occurs during JSON
     *                                                            parsing.
     */
    <T> T parseObject(byte[] jsonBytes, Class<T> valueType);

}