package com.appdynamics.extensions.wmb;

import static com.appdynamics.extensions.wmb.Util.convertToString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;

import org.slf4j.LoggerFactory;

import com.appdynamics.extensions.wmb.flowstats.FlowStatistics;
import com.appdynamics.extensions.wmb.flowstats.MessageFlow;
import com.appdynamics.extensions.wmb.flowstats.Node;
import com.appdynamics.extensions.wmb.flowstats.Terminal;
import com.appdynamics.extensions.wmb.flowstats.Thread;
import com.appdynamics.extensions.wmb.metrics.DefaultMetricProperties;
import com.appdynamics.extensions.wmb.metrics.Metric;
import com.appdynamics.extensions.wmb.metrics.MetricProperties;
import com.appdynamics.extensions.wmb.metrics.MetricValueTransformer;
import com.appdynamics.extensions.wmb.resourcestats.ResourceIdentifier;
import com.appdynamics.extensions.wmb.resourcestats.ResourceStatistics;
import com.appdynamics.extensions.wmb.resourcestats.ResourceType;
import com.singularity.ee.agent.systemagent.api.MetricWriter;

public class StatsProcessor {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(StatsProcessor.class);
	private static final String BROKER_LABEL = "brokerLabel";
	private static final String EXECUTION_GROUP_NAME = "executionGroupName";

	private Map config;
	private XmlParser<ResourceStatistics> resourceStatsParser;
	private XmlParser<FlowStatistics> flowStatsParser;
	private MetricPrinter printer;
	private final MetricValueTransformer valueTransformer = new MetricValueTransformer();
	private ResourceSubscriber resourceSubscriber = new ResourceSubscriber();
	private FlowSubscriber flowSubscriber = new FlowSubscriber();

	public StatsProcessor(Map config, XmlParser resourceStatsParser, XmlParser flowStatsParser, MetricPrinter printer) {
		this.config = config;
		this.printer = printer;
		this.resourceStatsParser = resourceStatsParser;
		this.flowStatsParser = flowStatsParser;
	}

	public void subscribe(Connection conn) throws JMSException {
		Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Object metricsObj = config.get("metrics");
		if (metricsObj != null) {

			Map metrics = (Map) metricsObj;

			// Subscribe to resource statistics
			if (metrics.get("resourceStatistics") != null) {
				List<Map> resourceStats = (List<Map>) metrics.get("resourceStatistics");
				for (Map resourceStat : resourceStats) {
					Topic topic = session.createTopic(convertToString(resourceStat.get("name"), ""));
					TopicSubscriber topicSub = session.createDurableSubscriber(topic,
							convertToString(resourceStat.get("subscriberName"), ""));
					topicSub.setMessageListener(resourceSubscriber);
				}
				logger.info("Resource Statistic Subscribers are registered.");
			}

			// Subscribe to message flow statistics
			if (metrics.get("flowStatistics") != null) {
				List<Map> flowStats = (List<Map>) metrics.get("flowStatistics");
				for (Map flowStat : flowStats) {
					Topic topic = session.createTopic(convertToString(flowStat.get("name"), ""));
					TopicSubscriber topicSub = session.createDurableSubscriber(topic,
							convertToString(flowStat.get("subscriberName"), ""));
					topicSub.setMessageListener(flowSubscriber);
				}
				logger.info("Mesage Flow Statistic Subscribers are registered.");
			}
		}
	}

	private class ResourceSubscriber implements MessageListener {

		public void onMessage(Message message) {
			long startTime = System.currentTimeMillis();
			String text = null;
			try {
				text = getMessageString(message);
				if (text != null) {
					try {
						ResourceStatistics resourceStatistics = resourceStatsParser.parse(text);
						if (resourceStatistics != null) {
							List<Metric> metrics = buildResourceMetrics(resourceStatistics);
							printer.reportMetrics(metrics);
						}
					} catch (JAXBException e) {
						logger.error("Unable to unmarshal XML message {}", text, e);
					}
				} else {
					logger.error("Message received is null.");
				}
			} catch (JMSException e) {
				logger.error("Unable to process message {}", e);
			}
			logger.debug("Time taken to process one message = {}",
					Long.toString(System.currentTimeMillis() - startTime));
		}
	}

	private class FlowSubscriber implements MessageListener {

		public void onMessage(Message message) {
			long startTime = System.currentTimeMillis();
			String text = null;
			try {
				text = getMessageString(message);
				if (text != null) {
					try {
						FlowStatistics flowStatistics = flowStatsParser.parse(text);
						if (flowStatistics != null) {
							List<Metric> metrics = buildFlowMetrics(flowStatistics);
							printer.reportMetrics(metrics);
						}
					} catch (JAXBException e) {
						logger.error("Unable to unmarshal XML message {}", text, e);
					}
				} else {
					logger.error("Message received is null.");
				}
			} catch (JMSException e) {
				logger.error("Unable to process message {}", e);
			}
			logger.debug("Time taken to process one message = {}",
					Long.toString(System.currentTimeMillis() - startTime));
		}
	}

	private List<Metric> buildResourceMetrics(ResourceStatistics resourceStatistics) {
		List<Metric> metrics = new ArrayList<Metric>();
		if (resourceStatistics != null) {
			String brokerName = resourceStatistics.getAttributes().get(new QName(BROKER_LABEL));
			String executionGroupName = resourceStatistics.getAttributes().get(new QName(EXECUTION_GROUP_NAME));
			if (resourceStatistics.getResourceType() != null) {
				for (ResourceType resourceType : resourceStatistics.getResourceType()) {
					String resourceTypeName = resourceType.getName();
					if (resourceType.getResourceIdentifiers() != null) {
						for (ResourceIdentifier resourceIdentifier : resourceType.getResourceIdentifiers()) {
							String resourceIdName = resourceIdentifier.getName();
							for (QName key : resourceIdentifier.getAttributes().keySet()) {
								String value = resourceIdentifier.getAttributes().get(key);
								String metricPath = formMetricPath(Arrays.asList(brokerName, executionGroupName,
										resourceTypeName, resourceIdName, key.toString()));
								MetricProperties metricProps = new DefaultMetricProperties();
								BigDecimal val = valueTransformer.transform(metricPath, value, metricProps);
								if (val != null) {
									Metric m = new Metric();
									m.setMetricName(key.toString());
									m.setMetricKey(metricPath);
									m.setProperties(metricProps);
									m.setMetricValue(val);
									metrics.add(m);
								}
							}
						}
					}
				}
			}
		}
		return metrics;
	}

	private List<Metric> buildFlowMetrics(FlowStatistics flowStatistics) {
		List<Metric> metrics = new ArrayList<Metric>();
		if (flowStatistics != null) {
			MessageFlow messageFlow = flowStatistics.getMessageFlow();
			if (messageFlow != null) {
				String brokerName = messageFlow.getAttributes().get(new QName("BrokerLabel"));
				String executionGroupName = messageFlow.getAttributes().get(new QName("ExecutionGroupName"));
				String applicationName = messageFlow.getAttributes().get(new QName("ApplicationName"));
				String flowName = messageFlow.getAttributes().get(new QName("MessageFlowName"));

				// Metric Path: <prefix>|<broker>|<execution group>|<application>|<message flow>|<metric name>
				// "Execution Group" is also called "Integration Server"
				String metricBasePath = formMetricPath(
						Arrays.asList(brokerName, executionGroupName, applicationName, flowName));
				MetricProperties defaultMetricProps = new DefaultMetricProperties();
				MetricProperties sumAggregationMetricProps = new DefaultMetricProperties();
				MetricProperties metricProps;
				sumAggregationMetricProps.setAggregationType(MetricWriter.METRIC_AGGREGATION_TYPE_SUM);
				sumAggregationMetricProps.setTimeRollupType(MetricWriter.METRIC_TIME_ROLLUP_TYPE_SUM);

				// Message Flow Metrics
				for (QName key : messageFlow.getAttributes().keySet()) {

					String value = messageFlow.getAttributes().get(key);
					String metricPath = new StringBuilder(metricBasePath).append("|").append(key).toString();
					
					metricProps = key.toString().startsWith("Total")
							|| key.toString().equals("ElapsedTimeWaitingForInputMessage")
							|| key.toString().equals("TimesMaximumNumberOfThreadsReached")
							? sumAggregationMetricProps : defaultMetricProps;

					Metric metricPoint = createMetricPoint(metricPath, value, metricProps, key.toString());
					if (metricPoint != null) {
						metrics.add(metricPoint);
					}
				}

				// Thread Metrics
				List<Thread> threads = flowStatistics.getThreadStatistics();
				if (threads != null) {
					for (Thread thread : threads) {

						String threadNumber = thread.getAttributes().get(new QName("Number"));

						for (QName key : thread.getAttributes().keySet()) {

							String value = thread.getAttributes().get(key);
							String metricPath = new StringBuilder(metricBasePath)
									.append("|Threads|" + threadNumber + "|").append(key).toString();

							metricProps = key.toString().startsWith("Total")
									|| key.toString().equals("ElapsedTimeWaitingForInputMessage")
											? sumAggregationMetricProps : defaultMetricProps;

							Metric metricPoint = createMetricPoint(metricPath, value, metricProps, key.toString());
							if (metricPoint != null) {
								metrics.add(metricPoint);
							}
						}
					}
				}

				// Node Metrics
				List<Node> nodes = flowStatistics.getNodeStatistics();
				if (nodes != null) {
					for (Node node : nodes) {
						String nodeLabel = node.getAttributes().get(new QName("Label"));
						String nodeType = node.getAttributes().get(new QName("Type"));

						for (QName key : node.getAttributes().keySet()) {

							String value = node.getAttributes().get(key);
							String metricPath = new StringBuilder(metricBasePath)
									.append("|Nodes|" + nodeLabel + " (" + nodeType + ")|").append(key).toString();

							metricProps = key.toString().startsWith("Total")
									|| key.toString().equals("CountOfInvocations")
									? sumAggregationMetricProps : defaultMetricProps;

							Metric metricPoint = createMetricPoint(metricPath, value, metricProps, key.toString());
							if (metricPoint != null) {
								metrics.add(metricPoint);
							}
						}

						// Terminal Metrics, only relevant attribute is
						// "CountOfInvocations"
						List<Terminal> terminals = node.getTerminalStatistics();
						if (terminals != null) {
							for (Terminal terminal : terminals) {
								String terminalLabel = terminal.getAttributes().get(new QName("Label"));
								String terminalType = terminal.getAttributes().get(new QName("Type"));

								String value = terminal.getAttributes().get("CountOfInvocations");
								String metricPath = new StringBuilder(metricBasePath).append("|Nodes|Terminals|"
										+ terminalLabel + " (" + terminalType + ")|CountOfInvocations").toString();

								Metric metricPoint = createMetricPoint(metricPath, value, sumAggregationMetricProps, "CountOfInvocations");
								metrics.add(metricPoint);
							}
						}
					}
				}
			}

		}
		return metrics;
	}

	private Metric createMetricPoint(String path, String value, MetricProperties properties, String name) {
		BigDecimal decimalValue = valueTransformer.transform(path, value, properties);

		if (decimalValue != null) {
			Metric m = new Metric();
			m.setMetricName(name);
			m.setMetricKey(path);
			m.setProperties(properties);
			m.setMetricValue(decimalValue);

			return m;
		} else {
			return null;
		}
	}

	private String formMetricPath(List<String> pathElements) {
		StringBuilder metricBuilder = new StringBuilder();

		if (pathElements.get(0) != null) {
			metricBuilder.append(pathElements.get(0));
		}

		for (int i = 1; i < pathElements.size(); i++) {
			String pathElement = pathElements.get(i);
			if (pathElement != null) {
				metricBuilder.append("|" + pathElement);
			}
		}

		return metricBuilder.toString();
	}

	private String getMessageString(Message message) throws JMSException {
		if (message != null) {
			if (message instanceof TextMessage) {
				TextMessage tm = (TextMessage) message;
				return tm.getText();
			} else if (message instanceof BytesMessage) {
				BytesMessage bm = (BytesMessage) message;
				byte data[] = new byte[(int) bm.getBodyLength()];
				bm.readBytes(data);
				return new String(data);
			}
		}
		
		throw new JMSException("Message is not of TextMessage/BytesMessage.");
	}
}
