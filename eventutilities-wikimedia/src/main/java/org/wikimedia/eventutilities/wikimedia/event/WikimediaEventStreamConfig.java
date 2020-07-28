package org.wikimedia.eventutilities.wikimedia.event;

import org.wikimedia.eventutilities.core.event.EventStreamConfig;

import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;

public class WikimediaEventStreamConfig extends EventStreamConfig {

    /**
     * Base MediaWiki EventStreamConfig API URL.
     * Default for EventStreamConfig class streamConfigsUriString param.
     */
    public final static String STREAM_CONFIG_URI_DEFAULT =
        "https://meta.wikimedia.org/w/api.php?format=json&action=streamconfigs&all_settings=true";

    /**
     * Format string used when building a MediaWiki EventStreamConfig API request for
     * a specific list of streams.  Default for EventStreamConfig class streamsParamFormat
     * parameter.
     */
    public final static String STREAMS_PARAM_FORMAT_DEFAULT = "&streams=%s";

    /**
     * streams param separator for MediaWiki EventStreamConfig API.
     * Default for EventStreamConfig class streamConfigsStreamsDelimiter parameter.
     */
    public final static String STREAMS_PARAM_DELIMITER_DEFAULT = "|";

    /**
     * Stream config topics setting.  Used to get a full list of topics for streams.
     */
    public static final String TOPICS_SETTING = "topics";

    /**
     * Stream Config setting name for schema title.
     */
    public static final String SCHEMA_TITLE_SETTING = "schema_title";

    /**
     * Stream Config setting name for destination event service name.
     */
    public static final String EVENT_SERVICE_SETTING = "destination_event_service";

    /**
     * Maps event service name to a service URL.
     * See also https://wikitech.wikimedia.org/wiki/Service_ports
     */
    protected HashMap<String, URI> eventServiceToUriMap;


    /**
     * TODO: We should be able to get puppet to render a
     * config file with a mapping of event service name to
     * event service urls.  That would be less fragile
     * than this hardcoded list.
     */
    public static final HashMap<String, URI> EVENT_SERVICE_TO_URI_MAP_DEFAULT =
        new HashMap<String, URI>() {{
            put("eventgate-main", URI.create("https://eventgate-main.discovery.wmnet:4492/v1/events"));
            put("eventgate-main-eqiad", URI.create("https://eventgate-main.svc.eqiad.wmnet:4492/v1/events"));
            put("eventgate-main-codfw", URI.create("https://eventgate-main.svc.codfw.wmnet:4492/v1/events"));

            put("eventgate-analytics", URI.create("https://eventgate-analytics.discovery.wmnet:4592/v1/events"));
            put("eventgate-analytics-eqiad", URI.create("https://eventgate-analytics.svc.eqiad.wmnet:4592/v1/events"));
            put("eventgate-analytics-codfw", URI.create("https://eventgate-analytics.svc.codfw.wmnet:4592/v1/events"));

            put("eventgate-analytics-external", URI.create("https://eventgate-analytics-external.discovery.wmnet:4692/v1/events"));
            put("eventgate-analytics-external-eqiad", URI.create("https://eventgate-analytics-external.svc.eqiad.wmnet:4692/v1/events"));
            put("eventgate-analytics-external-codfw", URI.create("https://eventgate-analytics-external.svc.codfw.wmnet:4692/v1/events"));

            put("eventgate-logging-external", URI.create("https://eventgate-logging-external.discovery.wmnet:4392/v1/events"));
            put("eventgate-logging-external-eqiad", URI.create("https://eventgate-logging-external.svc.eqiad.wmnet:4392/v1/events"));
            put("eventgate-logging-external-codfw", URI.create("https://eventgate-logging-external.svc.codfw.wmnet:4392/v1/events"));
        }};

    /**
     * streamConfigsUriString           default: "https://meta.wikimedia.org/w/api.php?format=json&action=streamconfigs&all_settings=true"
     * streamConfigsStreamsParamFormat  default: "&streams=%s"
     * streamConfigsStreamsDelimiter    default: "|"
     * eventServiceToUriMap:            default: EVENT_SERVICE_TO_URI_MAP_DEFAULT
     */
    public WikimediaEventStreamConfig() {
        this(STREAM_CONFIG_URI_DEFAULT);
    }

    /**
     * streamConfigsStreamsParamFormat  default: "&streams=%s"
     * streamConfigsStreamsDelimiter    default: "|"
     * eventServiceToUriMap:            default: EVENT_SERVICE_TO_URI_MAP_DEFAULT
     *
     * @param streamConfigsUriString
     */
    public WikimediaEventStreamConfig(String streamConfigsUriString) {
        this(streamConfigsUriString, STREAMS_PARAM_FORMAT_DEFAULT);
    }

    /**
     * streamConfigsStreamsDelimiter    default :"|"
     * eventServiceToUriMap:            default: EVENT_SERVICE_TO_URI_MAP_DEFAULT*
     *
     * @param streamConfigsUriString
     * @param streamConfigsStreamsParamFormat
     */
    public WikimediaEventStreamConfig(
        String streamConfigsUriString,
        String streamConfigsStreamsParamFormat
    ) {
        this(
            streamConfigsUriString,
            streamConfigsStreamsParamFormat,
            STREAMS_PARAM_DELIMITER_DEFAULT
        );
    }

    /**
     * eventServiceToUriMap:            default: EVENT_SERVICE_TO_URI_MAP_DEFAULT*
     *
     * @param streamConfigsUriString
     * @param streamConfigsStreamsParamFormat
     * @param streamConfigsStreamsDelimiter
     */
    public WikimediaEventStreamConfig(
        String streamConfigsUriString,
        String streamConfigsStreamsParamFormat,
        String streamConfigsStreamsDelimiter
    ) {
        this(
            streamConfigsUriString,
            streamConfigsStreamsParamFormat,
            streamConfigsStreamsDelimiter,
            EVENT_SERVICE_TO_URI_MAP_DEFAULT
        );
    }

    /**
     *
     * @param streamConfigsUriString
     *  http://, file:// or other loadable URI.
     *
     * @param streamConfigsStreamsParamFormat
     *  Format string passed to String.format to format the 'streams' parameter onto
     *  streamConfigsUriString.
     *  If you are loading from a REST API, you might want to change this to /streams/%s.
     *
     * @param streamConfigsStreamsDelimiter
     *  Used to join requested stream names into the streams param.
     *
     * @param eventServiceToUriMap
     *  Maps event service names to event service URIs.
     *
     */
    public WikimediaEventStreamConfig(
        String streamConfigsUriString,
        String streamConfigsStreamsParamFormat,
        String streamConfigsStreamsDelimiter,
        HashMap<String, URI> eventServiceToUriMap
    ) {
        this(
            (streamNames) -> {
                String streamsParamDelimiter;
                try {
                    // We need to support streams param delimiters like "|", which is what
                    // MW API expects, but URI.create will throw a
                    // java.lang.IllegalArgumentException: Illegal character
                    // if we don't URL encode "|" first.
                    streamsParamDelimiter = URLEncoder.encode(
                        streamConfigsStreamsDelimiter, "UTF-8"
                    );
                } catch (java.io.UnsupportedEncodingException e) {
                    throw new RuntimeException(
                        "Could not URL encode " + streamConfigsStreamsDelimiter +
                            ". " + e.getMessage()
                    );
                }

                String streamsParam = String.format(
                    streamConfigsStreamsParamFormat,
                    String.join(streamsParamDelimiter, streamNames)
                );

                return URI.create(
                    streamConfigsUriString + streamsParam
                );
            },
            eventServiceToUriMap
        );
    }

    /**
     * Instantiates a WikimediaEventStreamConfig with a custom makeStreamConfigsUriLambda.
     * @param makeStreamConfigsUriLambda
     */
    public WikimediaEventStreamConfig(
        EventStreamConfig.MakeStreamConfigsUri makeStreamConfigsUriLambda,
        HashMap<String, URI> eventServiceToUriMap
    ) {
        super(makeStreamConfigsUriLambda);
        this.eventServiceToUriMap = eventServiceToUriMap;
    }

    /**
     * Get all topics settings for the a single stream.
     * @param streamName
     * @return
     */
    public String getSchemaTitle(String streamName) {
        return getSettingAsString(streamName, SCHEMA_TITLE_SETTING);
    }

    /**
     * Get all topics settings for all known streams.
     * @return
     */
    public List<String> getAllCachedTopics() {
        return collectAllCachedSettingsAsString(TOPICS_SETTING);
    }

    /**
     * Get all topics settings for the a single stream.
     * @param streamName
     * @return
     */
    public List<String> getTopics(String streamName) {
        return collectSettingAsString(streamName, TOPICS_SETTING);
    }

    /**
     * Get all topics settings for the list of specified streams.
     * @param streamNames
     * @return
     */
    public List<String> collectTopics(List<String> streamNames) {
        return collectSettingsAsString(streamNames, TOPICS_SETTING);
    }

    /**
     * Gets the destination_event_service name for the specified stream.
     * @param streamName
     * @return
     */
    public String getEventServiceName(String streamName) {
        return getSetting(streamName, EVENT_SERVICE_SETTING).asText();
    }

    /**
     * Gets the event service POST URI of an event service.
     * The URI is looked up in the eventServiceToUriMap.
     * If this the eventServiceName is not defined there, returns null.
     *
     * Note that this function is not looking up anything in the source stream
     * configuration; this is a static configuration provided to this EventStreamConfig
     * class constructor that maps from destination_event_service to a URI.
     * @param eventServiceName
     * @return
     */
    public URI getEventServiceUriByName(String eventServiceName) {
        return eventServiceToUriMap.get(eventServiceName);
    }

    /**
     * Gets the default event serivce URI for this stream via the EVENT_SERVICE_SETTING.
     * @param streamName
     * @return
     */
    public URI getEventServiceUri(String streamName) {
        return eventServiceToUriMap.get(getEventServiceName(streamName));
    }

    /**
     * Gets a datacenter specific destination event service URI for this stream
     * via the EVENT_SERVICE_SETTING + the datacenter name.
     * @param streamName
     * @param datacenter
     * @return
     */
    public URI getEventServiceUri(String streamName, String datacenter) {
        String destinationEventServiceName = getEventServiceName(streamName);
        String datacenterSpecificEventServiceName = destinationEventServiceName + "-" + datacenter;
        return getEventServiceUri(datacenterSpecificEventServiceName);
    }

}

