package org.wikimedia.eventutilities.core.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.load.SchemaLoader;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton class to handle fetching JSON schemas from URIs,
 * parsing them into JsonNodes, and caching them.
 * URIs can be local file:// URIs or remote HTTP:// URIs, or anything that
 * jackson.dataformat.yaml.YAMLParser can load.  If the data starts with a { or [
 * character, it will be assumed to be json and JsonParser will be used.
 * Otherwise YAMLParser will be used.  JSON data can contain certain unicode
 * characters that YAML cannot, so it is best to use JsonParser when we can.
 *
 * Usage:
 *
 * JsonSchemaLoader schemaLoader = JsonSchemaLoader.getInstance();
 * JsonNode schema = schemaLoader.load("http://my.schemas.org/schemas/test/event/schema/0.0.2")
 */
public class JsonSchemaLoader {

    static final JsonSchemaLoader instance = new JsonSchemaLoader();

    final ConcurrentHashMap<URI, com.fasterxml.jackson.databind.JsonNode> cache = new ConcurrentHashMap<>();

    final SchemaLoader schemaLoader = new SchemaLoader();

    public JsonSchemaLoader() { }

    public static JsonSchemaLoader getInstance() {
        return instance;
    }

    /**
     * Given a schemaUri, this will request the JSON or YAML content at that URI and
     * parse it into a JsonNode.  $refs will be resolved.
     * The compiled schema will be cached by schemaURI, and only looked up once per schemaURI.
     *
     * @param schemaUri
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) throws JsonLoadingException {
        if (this.cache.containsKey(schemaUri)) {
            return this.cache.get(schemaUri);
        }

        // Use SchemaLoader so we resolve any JsonRefs in the JSONSchema.
        JsonLoader jsonLoader = JsonLoader.getInstance();
        JsonNode schema = this.schemaLoader.load(jsonLoader.load(schemaUri)).getBaseNode();
        this.cache.put(schemaUri, schema);
        return schema;
    }

    /**
     * Parses the JSON or YAML string into a JsonNode.
     * @param data JSON or YAML string to parse into a JsonNode.
     * @return
     */
    public JsonNode parse(String data) throws JsonLoadingException {
        return JsonLoader.getInstance().parse(data);
    }

    /**
     * Proxy method to see if the schemaUri is currently cached.
     * @param schemaUri
     * @return
     */
    public boolean isCached(URI schemaUri) {
        return this.cache.containsKey(schemaUri);
    }

    /**
     * Proxy method to get a schema by schemaUri directly from the local cache.
     * @param schemaUri
     * @return
     */
    public JsonNode cacheGet(URI schemaUri) {
        return this.cache.get(schemaUri);
    }

    /**
     * Proxy method to put a schema by schemaUri direclty in the local cache.
     * @param schemaUri
     * @param schema
     * @return
     */
    public JsonNode cachePut(URI schemaUri, JsonNode schema) {
        return this.cache.put(schemaUri, schema);
    }

}
