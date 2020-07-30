package org.wikimedia.eventutilities.core.event;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Class to aide in constructing EventStream instances and
 * working with groups of event streams using
 * EventStreamConfig API and event schema repositories.
 */
public class EventStreamFactory {

    /**
     * EventSchemaLoader instance used when constructing EventStreams.
     */
    protected final EventSchemaLoader eventSchemaLoader;

    /**
     * EventStreamConfig instance used when constructing EventStreams and
     * looking up stream configs.
     */
    protected EventStreamConfig eventStreamConfig;

    /**
     * Constructs a new EventStreamFactory with new instances of
     * EventSchemaLoader and EventStreamConfig with the given URIs.
     * @param schemaBaseUris
     * @param eventStreamConfig
     */
    public EventStreamFactory(
        List<String> schemaBaseUris,
        EventStreamConfig eventStreamConfig
    ) {
        this(
            new EventSchemaLoader(schemaBaseUris),
            eventStreamConfig
        );
    }

    /**
     * Constructs a new instance of EventStreamFactory.
     * @param eventSchemaLoader
     * @param eventStreamConfig
     */
    public EventStreamFactory(
        EventSchemaLoader eventSchemaLoader,
        EventStreamConfig eventStreamConfig
    ) {
        this.eventSchemaLoader = eventSchemaLoader;
        this.eventStreamConfig = eventStreamConfig;
    }

    public static EventStreamFactory createMediawikiConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String mediawikiApiEndpoint,
        HashMap<String, URI> eventServiceToUriMap
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createMediawikiEventStreamConfig(
                mediawikiApiEndpoint,
                eventServiceToUriMap
            )
        );
    }

    public static EventStreamFactory createMediawikiConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String mediawikiApiEndpoint
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createMediawikiEventStreamConfig(
                mediawikiApiEndpoint
            )
        );
    }

    public static EventStreamFactory createMediawikiConfigEventStreamFactory(
        List<String> schemaBaseUris
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createMediawikiEventStreamConfig()
        );
    }


    public static EventStreamFactory createStaticConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String streamConfigsUriString,
        HashMap<String, URI> eventServiceToUriMap
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createStaticEventStreamConfig(
                streamConfigsUriString,
                eventServiceToUriMap
            )
        );
    }

    public static EventStreamFactory createStaticConfigEventStreamFactory(
        List<String> schemaBaseUris,
        String streamConfigsUriString
    ) {
        return new EventStreamFactory(
            schemaBaseUris,
            EventStreamConfigFactory.createStaticEventStreamConfig(
                streamConfigsUriString
            )
        );
    }


    /**
     * Returns a new EventStream for this streamName using eventSchemaLoader and
     * eventStreamConfig.
     * @param streamName
     * @return
     */
    public EventStream createEventStream(String streamName) {
        return new EventStream(streamName, eventSchemaLoader, eventStreamConfig);
    }

    /**
     * Returns a List of new EventStreams using eventSchemaLoader nad
     * eventStreamConfig.
     * @param streamNames
     * @return
     */
    public List<EventStream> createEventStreams(List<String> streamNames) {
        return streamNames.stream()
            .map(this::createEventStream)
            .collect(Collectors.toList());
    }

    /**
     * Returns the EventStreamConfig instance this EventStreamFactory is using.
     * @return
     */
    public EventStreamConfig getEventStreamConfig() {
        return eventStreamConfig;
    }

    /**
     * Returns the EventSchemaLoader instance this EventStreamFactory is using.
     * @return
     */
    public EventSchemaLoader getEventSchemaLoader() {
        return eventSchemaLoader;
    }

}






