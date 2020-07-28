package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.wikimedia.eventutilities.core.json.JsonLoader;

/**
 * Class to fetch and work with stream configuration from the a URI.
 * Upon instantiation, this will attempt to pre fetch and cache all stream config from
 * streamConfigsUri.  Further accesses of uncached stream names will cause
 * a fetch from the result of makeStreamConfigsUriForStreams for the uncached stream names only.
 */
public class EventStreamConfig {
    /**
     * Base EventStreamConfig API URL.
     */
    protected URI streamConfigsUri;

    public interface MakeStreamConfigsUri {
        URI apply(List<String> streamNames);
    }

    protected MakeStreamConfigsUri makeStreamConfigsUri;

    /**
     * Format string used when building an EventStreamConfig API request for
     * a specific list of streams.
     */
    protected String streamsParamFormat;

    /**
     * Used when joining a list of specific streams into the value of the streams param format.
     */
    protected String streamsParamDelimiter;

    /**
     * Cached stream configurations. This maps stream name (or stream pattern regex)
     * to stream config settings.
     */
    protected ObjectNode streamConfigsCache;

    /**
     * Constructs a simple EventStreamConfig instance that does not support
     * per stream config lookups.  This will just load the content at the
     * streamConfigsUriString on instantiation.
     * @param streamConfigsUriString
     */
    public EventStreamConfig(
        String streamConfigsUriString
    ) {
        this((streamNames) -> URI.create(streamConfigsUriString));
    }

    /**
     * @param makeStreamConfigsUriLambda
     *  Lambda function.  Given a List<String> of stream names, this should
     *  return a URI from which stream configs for those streams should be requested.
     *  If given an empty list, or null, this should return a URI that should be requested
     *  to get all stream configs.
     */
    public EventStreamConfig(
        MakeStreamConfigsUri makeStreamConfigsUriLambda
    ) {
        this.makeStreamConfigsUri = makeStreamConfigsUriLambda;

        // Get and store all known stream configs.
        // The stream configs endpoint should return an object mapping
        // stream names (or regex patterns) to stream config entries.
        this.reset();
    }

    /**
     * Re-fetches the content for all stream configs and saves it in the local
     * stream configs cache.  This should fetch and cache all stream configs.
     */
    public void reset() {
        streamConfigsCache = fetch(getStreamConfigsUri());
    }

    /**
     * Loads the JSON stream configs result from uri
     * @param uri
     * @return JsonNode parsed from the value at streamConfigsUri
     */
    protected static ObjectNode fetch(URI uri) {
        return (ObjectNode)JsonLoader.get(uri);
    }

    /**
     * Calls makeStreamConfigsUri lambda with an empty List of stream names.
     * @return
     */
    public URI getStreamConfigsUri() {
        return makeStreamConfigsUri.apply(new ArrayList<String>());
    }

    /**
     * Calls makeStreamConfigsUri lambda with a list of stream names.
     * @param streamNames
     * @return
     */
    public URI getStreamConfigsUriForStream(List<String> streamNames) {
        return makeStreamConfigsUri.apply(streamNames);
    }

    /**
     * Returns a Java Stream iterator over the stream config entries.
     * Useful for e.g. map, filter, reduce etc.
     * @return
     */
    public Stream<JsonNode> elementsStream() {
        return StreamSupport.stream(streamConfigsCache.spliterator(), false);
    }

    /**
     * Returns a Java Stream iterator over the Map.Entry of stream name -> stream config entries.
     * Useful for e.g. map, filter, reduce etc.
     * @return
     */
    public Stream<Map.Entry<String,JsonNode>> fieldsStream() {
        return StreamSupport.stream(
            Spliterators.spliterator(
                streamConfigsCache.fields(),
                streamConfigsCache.size(),
                Spliterator.SIZED | Spliterator.IMMUTABLE
            ),
            false
        );
    }

    /**
     * Returns all cached stream configs.
     * @return
     */
    public ObjectNode cachedStreamConfigs() {
        return streamConfigsCache.deepCopy();
    }

    /**
     * Returns all cached stream name keys.
     * @return
     */
    public List<String> cachedStreamNames() {
        List<String> streamNames = new ArrayList<>();
        streamConfigsCache.fieldNames().forEachRemaining(streamNames::add);
        return streamNames;
    }

    /**
     * Returns the stream config entry for a specific stream.
     * This will still return an ObjectNode mapping the
     * stream name to the stream config entry. E.g.
     *
     *   getStreamConfigs(my_stream) ->
     *     { my_stream: { schema_title: my/schema, ... } }
     *
     * @param streamName
     * @return
     */
    public ObjectNode getStreamConfig(String streamName) {
        return getStreamConfigs(Collections.singletonList(streamName));
    }

    /**
     * Gets the stream config entries for the desired stream names.
     * Returns a JsonNode map of stream names to stream config entries.
     * @param streamNames
     * @return
     */
    public ObjectNode getStreamConfigs(List<String> streamNames) {
        // If any of the desired streams are not cached, try to fetch them now and cache them.
        List<String> unfetchedStreams = streamNames.stream()
            .filter(streamName -> !streamConfigsCache.has(streamName))
            .collect(Collectors.toList());

        if (!unfetchedStreams.isEmpty()) {
            ObjectNode fetchedStreamConfigs = fetch(getStreamConfigsUriForStream(unfetchedStreams));
            streamConfigsCache.setAll(fetchedStreamConfigs);
        }

        // Return only desired stream configs.
        return streamConfigsCache.deepCopy().retain(streamNames);
    }

    /**
     * Gets a stream config setting for a specific stream.  E.g.
     *
     * JsonNode setting = getSetting("mediawiki.revision-create", "destination_event_service")
     *  -> TextNode("eventgate-main")
     * You'll still have to pull the value out of the JsonNode wrapper yourself.
     * E.g. setting.asText() or setting.asDouble()
     *
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     *
     * @param streamName
     * @param settingName
     * @return
     */
    public JsonNode getSetting(String streamName, String settingName) {
        JsonNode streamConfigEntry = getStreamConfig(streamName).get(streamName);
        if (streamConfigEntry == null) {
            return null;
        } else {
            return streamConfigEntry.get(settingName);
        }
    }

    /**
     * Gets the stream config setting for a specific stream as a string.
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     *
     * JsonNode setting = getSettingAsString("mediawiki.revision-create", "destination_event_service")
     *  -> "eventgate-main"
     *
     * If either this streamName does not have a stream config entry, or
     * the stream config entry does not have setting, this returns null.
     *
     * @param streamName
     * @param settingName
     * @return
     */
    public String getSettingAsString(String streamName, String settingName) {
        JsonNode settingNode = getSetting(streamName, settingName);
        if (settingNode == null) {
            return null;
        } else {
            return settingNode.asText();
        }
    }

    /**
     * Collects the settingName value for streamName.  If the setting value is an array,
     * it will be flattened into a list of JsonNode values.
     *
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } -> [val1, val2, val3, val4]
     *  collectSetting("stream2", "setting1") -> [JsonNode("val3"), JsonNode("val4")]
     *
     * @param streamName
     * @param settingName
     * @return
     */
    public List<JsonNode> collectSetting(String streamName, String settingName) {
        return collectSettings(Collections.singletonList(streamName), settingName);
    }

    /**
     * Collects the settingName value as a String for streamName.  If the setting value is an array,
     * it will be flattened into a list of String values.
     *
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } -> [val1, val2, val3, val4]
     *  collectSettingAsString("stream2", "setting1") -> ["val3", "val4"]
     *
     * @param streamName
     * @param settingName
     * @return
     */
    public List<String> collectSettingAsString(String streamName, String settingName) {
        return jsonNodesAsText(collectSetting(streamName, settingName));
    }

    /**
     * Collects all settingName values for each of the listed streamNames.  If any
     * encountered setting values is an array, it will be flattened.
     *
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } -> [val1, val2, val3, val4]
     *  collectSettings(["stream1", "stream2"], "setting1") -> [JsonNode("val1"), JsonNode("val2"), JsonNode("val3"), JsonNode("val4")]
     *
     * @param streamNames
     * @param settingName
     * @return
     */
    public List<JsonNode> collectSettings(List<String> streamNames, String settingName) {
        return objectNodeCollectValues(getStreamConfigs(streamNames), settingName);
    }

    /**
     * Collects all settingName values as a String for each of the listed streamNames.  If any
     * encountered setting values is an array, it will be flattened.
     *
     *   { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } -> [val1, val2, val3, val4]
     *  collectSettingsAsString(["stream1", "stream2"], "setting1") -> ["val1", "val2", "val3", "val4"]
     *
     * @param streamNames
     * @param settingName
     * @return
     */
    public List<String> collectSettingsAsString(List<String> streamNames, String settingName) {
        return jsonNodesAsText(collectSettings(streamNames, settingName));
    }

    /**
     * Collects all settingName values of every cached stream config entry.
     * If the value is an array, its contents will be flattened.
     *
     *  { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } -> [val1, val2, val3, val4]
     *  collectAllCachedSettings("setting1") -> [JsonNode("val1"), JsonNode("val2"), JsonNode("val3"), JsonNode("val4")]
     *
     * @param settingName
     * @return
     */
    public List<JsonNode> collectAllCachedSettings(String settingName) {
        return objectNodeCollectValues(streamConfigsCache, settingName);
    }

    /**
     * Collects all settingName values of every cached stream config entry as a String
     * If the value is an array, its contents will be flattened.
     *
     *   { stream1: { setting1: [val1, val2] }, stream2: { setting1: [val3, val4] } -> [val1, val2, val3, val4]
     *  collectAllCachedSettingsAsString(setting1") -> ["val1", "val2", "val3", "val4"]
     *
     * @param settingName
     * @return
     */
    public List<String> collectAllCachedSettingsAsString(String settingName) {
        return jsonNodesAsText(collectAllCachedSettings(settingName));
    }

    /**
     * Finds all values of fieldName of each element in objectNode.
     * If the found value is an array, its contents will be flattened.
     *
     * E.g.
     * { key1: { targetField: val1 }, key2: { targetField: val2 } } -> [val1, val2]
     * { key1: { targetField: [val1, val2] }, key2: { targetField: [val3, val4] } -> [val1, val2, val3, val4]
     *
     * @param objectNode
     * @param fieldName
     * @return
     */
    protected static List<JsonNode> objectNodeCollectValues(ObjectNode objectNode, String fieldName) {
        List<JsonNode> results = new ArrayList<>();

        for (JsonNode jsonNode : objectNode.findValues(fieldName)) {
            if (jsonNode.isArray()) {
                jsonNode.forEach(results::add);
            } else {
                results.add(jsonNode);
            }
        }
        return results;
    }

    /**
     * Converts a List of JsonNodes to a List of Strings using JsonNode::asText.
     * @param jsonNodes
     * @return
     */
    protected static List<String> jsonNodesAsText(List<JsonNode> jsonNodes) {
        return jsonNodes.stream()
            .map(JsonNode::asText)
            .collect(Collectors.toList());
    }

}
