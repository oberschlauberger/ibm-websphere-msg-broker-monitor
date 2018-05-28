package com.appdynamics.extensions.wmb.resourcestats;

import com.appdynamics.extensions.wmb.StatsProcessor;
import com.appdynamics.extensions.wmb.XmlParser;
import com.appdynamics.extensions.wmb.metricUtils.Metric;
import com.appdynamics.extensions.wmb.metricUtils.MetricPrinter;
import com.appdynamics.extensions.wmb.metricUtils.MetricProperties;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.appdynamics.extensions.wmb.Util.convertToString;
import static com.appdynamics.extensions.wmb.Util.join;

public class ResourceStatsProcessor<T> extends StatsProcessor<T> implements MessageListener {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ResourceStatsProcessor.class);
    private static final String EXECUTION_GROUP_NAME = "executionGroupName";

    private Map<String,MetricProperties> metricPropsHolder;

    public ResourceStatsProcessor(Map config, XmlParser<T> parser, MetricPrinter printer) {
        super(config,parser,printer);
        this.metricPropsHolder = buildMetricProperties("metrics","resourceStatistics");
    }

    public void subscribe(TopicSession session) throws JMSException {
        if (config.get("resourceStatisticsSubscribers") != null) {
            List<Map> resourceSubscribers = (List<Map>) config.get("resourceStatisticsSubscribers");
            for (Map resourceSub : resourceSubscribers) {
                String topicPath = convertToString(resourceSub.get("topic"), "");
                Boolean durable = Boolean.valueOf(convertToString(resourceSub.get("durable"), "false"));
                Topic topic = session.createTopic(topicPath);
                TopicSubscriber topicSub;
                if (durable) {
                    String subscriberName = convertToString(resourceSub.get("subscriberName"), "");
                    topicSub = session.createDurableSubscriber(topic, subscriberName);
                    logger.info("Registered durable subscriber \"" + subscriberName + "\" for topic \"" + topicPath + "\"");
                } else {
                    topicSub = session.createSubscriber(topic);
                    logger.info("Registered non-durable subscriber for topic \"" + topicPath + "\"");
                }
                topicSub.setMessageListener(this);
            }
            logger.info("Resource statistic subscribers are registered.");
        }
    }

    /**
     * This method is called every time when there is a message on the topic.
     * @param message
     */
    public void onMessage(Message message) {
    	long startTime = System.currentTimeMillis();
        String text = null;
        try {
        	text = getMessageString(message);
            if(text != null) {
                try {
                    T resourceStatistics = parser.parse(text);
                    if (resourceStatistics != null) {
                        List<Metric> metrics = buildMetrics(resourceStatistics);
                        printer.reportMetrics(metrics);
                    }
                } catch (JAXBException e) {
                    logger.error("Unable to unmarshal XML message {}", text,e);
                }
            }
            else{
                logger.error("Message received is null.");
            }
        } catch(JMSException e){
            logger.error("Unable to process message {}", e);
        }
        logger.debug("Time taken to process one message = {}" ,Long.toString(System.currentTimeMillis() - startTime));
    }

    protected List<Metric> buildMetrics(T stats) {
        ResourceStatistics resourceStatistics = (ResourceStatistics) stats;
        List<Metric> metrics = new ArrayList<Metric>();
        if(resourceStatistics != null){
            //String brokerName = resourceStatistics.getAttributes().get(new QName(BROKER_LABEL));
            String executionGroupName = resourceStatistics.getAttributes().get(new QName(EXECUTION_GROUP_NAME));
            if(resourceStatistics.getResourceType() != null){
                for(ResourceType resourceType : resourceStatistics.getResourceType()){
                    String resourceTypeName = resourceType.getName();
                    if(resourceType.getResourceIdentifiers() != null){
                        for(ResourceIdentifier resourceIdentifier : resourceType.getResourceIdentifiers()){
                            String resourceIdName = resourceIdentifier.getName();
                            for (QName key: resourceIdentifier.getAttributes().keySet()) {
                                String resourceMetric = join(SEPARATOR,resourceTypeName,resourceIdName,key.toString());
                                MetricProperties resourceMetricProps = metricPropsHolder.get(resourceMetric);
                                if(resourceMetricProps != null){
                                    String value = resourceIdentifier.getAttributes().get(key);
                                    String metricPath = join(SEPARATOR,executionGroupName,
                                            "Resource Statistics", resourceTypeName, resourceIdName);
                                    Metric metricPoint = createMetricPoint(metricPath,value,resourceMetricProps,key.toString());
                                    if(metricPoint != null){
                                        metrics.add(metricPoint);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return metrics;
    }
}
