package com.appdynamics.extensions.wmb.resourceStats;

import com.appdynamics.extensions.util.MetricWriteHelper;
import com.appdynamics.extensions.wmb.MetricPrinter;
import com.appdynamics.extensions.wmb.ParserFactory;
import com.appdynamics.extensions.wmb.StatsProcessor;
import com.appdynamics.extensions.wmb.XmlParser;
import com.appdynamics.extensions.wmb.resourcestats.ResourceIdentifier;
import com.appdynamics.extensions.wmb.resourcestats.ResourceStatistics;
import com.appdynamics.extensions.wmb.resourcestats.ResourceType;
import com.appdynamics.extensions.yml.YmlReader;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class ResourceStatProcessorTest {

	ParserFactory parserFactory = new ParserFactory();

	@Test
	public void canParseXmlMessageSuccessfully() throws IOException, JMSException, JAXBException {
		MetricWriteHelper writer = mock(MetricWriteHelper.class);
		StatsProcessor processor = getResourceStatProcessor(writer);
		TextMessage mockMsg = mock(TextMessage.class);
		when(mockMsg.getText()).thenReturn(getFileContents("/resourceStats.xml"));
		ArgumentCaptor<String> metricPathCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
		getResourceSubscriberFromProcessor(processor).onMessage(mockMsg);
		verify(writer, atLeastOnce()).printMetric(metricPathCaptor.capture(), valueCaptor.capture(), anyString(),
				anyString(), anyString());
		Assert.assertTrue(metricPathCaptor.getAllValues()
				.contains("Custom Metrics|WMB|QMgr1|IB9NODE|default|JVM|summary|InitialMemoryInMB"));
		Assert.assertTrue(metricPathCaptor.getAllValues()
				.contains("Custom Metrics|WMB|QMgr1|IB9NODE|default|Parsers|[Administration]|ApproxMemKB"));
		Assert.assertTrue(valueCaptor.getAllValues().contains("72"));

	}

	@Test
	public void shouldNotThrowErrorWhenMessageIsNull() throws IOException, JMSException, JAXBException {
		MetricWriteHelper writer = mock(MetricWriteHelper.class);
		StatsProcessor processor = getResourceStatProcessor(writer);
		TextMessage mockMsg = mock(TextMessage.class);
		when(mockMsg.getText()).thenReturn(null);
		ArgumentCaptor<String> metricPathCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
		getResourceSubscriberFromProcessor(processor).onMessage(mockMsg);
		verify(writer, never()).printMetric(metricPathCaptor.capture(), valueCaptor.capture(), anyString(), anyString(),
				anyString());
	}

	private String getFileContents(String filepath) throws IOException {
		String filename = this.getClass().getResource(filepath).getFile();
		String text = Files.toString(new File(filename), Charsets.UTF_8);
		return text;
	}

	private StatsProcessor getResourceStatProcessor(MetricWriteHelper writer) throws JAXBException {
		Map configMap = YmlReader
				.readFromFileAsMap(new File(this.getClass().getResource("/conf/config.yml").getFile()));
		List<Map> qMgrs = (List<Map>) configMap.get("queueManagers");
		Map qMgrConfig = qMgrs.get(0);
		MetricPrinter printer = new MetricPrinter("Custom Metrics|WMB", "QMgr1", writer);
		StatsProcessor processor = new StatsProcessor(qMgrConfig, parserFactory.getResourceStatisticsParser(),
				parserFactory.getFlowStatisticsParser(), printer);
		return processor;
	}

	private MessageListener getResourceSubscriberFromProcessor(StatsProcessor processor) {
		MessageListener listener = null;
		try {
			Field resourceSubsciberField;
			resourceSubsciberField = processor.getClass().getDeclaredField("resourceSubscriber");
			resourceSubsciberField.setAccessible(true);
			listener = (MessageListener) resourceSubsciberField.get(processor);
		} catch (NoSuchFieldException e) {}
		catch (SecurityException e) {}
		catch (IllegalArgumentException e) {}
		catch (IllegalAccessException e) {}
		return listener;
	}
}