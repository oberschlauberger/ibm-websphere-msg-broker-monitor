package com.appdynamics.extensions.wmb;


import com.appdynamics.extensions.util.MetricWriteHelper;
import com.appdynamics.extensions.wmb.metricUtils.MetricPrinter;
import com.ibm.mq.MQException;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.xml.bind.JAXBException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.appdynamics.extensions.wmb.Util.convertToString;

class WMBMonitorTask implements Runnable{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(WMBMonitorTask.class);

    private String displayName;

    /* metric prefix from the config.yaml to be applied to each metric path*/
    private String metricPrefix;

    /* a facade to report metricUtils to the machine agent.*/
    private MetricWriteHelper metricWriter;

    private Map queueManagerConfig;

    private WMBMonitorTaskShutdownHandler shutdownHandler;

    private int initialDelayInMinutes = 0;

    private WMBMonitorTask(){
    }

    public void run() {

        if (initialDelayInMinutes > 0) {
            try {
                TimeUnit.MINUTES.sleep(initialDelayInMinutes);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            logger.info("Executing a run of WMBMonitor.");
            displayName = convertToString(queueManagerConfig.get("name"),"");
            MetricPrinter metricPrinter = new MetricPrinter(metricPrefix,displayName,metricWriter);
            final Connection conn = new ConnectionFactory().createConnection(queueManagerConfig);
            //subscribe subscribers
            StatsSubscription sub = new StatsSubscription(queueManagerConfig,metricPrinter);
            sub.subscribe(conn);

            conn.setExceptionListener(new ExceptionListener() {
                public void onException(JMSException e) {
                    MQException le = (MQException) e.getLinkedException();
                    if (le.getReason() == 2161) {
                        logger.error("QM is quiescing, exiting..");
                        try {
                            // still necessary: close the connection
                            conn.close();
                        } catch (JMSException expectedException) {
                            // this exception is expected when we close the connection while the QM is quiescing
                            // do nothing
                        } finally {
                            // schedule task for restart (reconnect)
                            shutdownHandler.onTaskShutdown(WMBMonitorTask.this);
                        }
                    } else {
                        logger.error("Connection to QM is broken..",e);
                    }
                }
            });

            //start connection
            conn.start();

        } catch (JMSException e) {
            logger.error("Unable to connect to the queue manager with name={}",displayName,e);
            // QM might be temporarily offline, this task will be scheduled for a restart in 1 minute
            shutdownHandler.onTaskShutdown(this);
        } catch (JAXBException e) {
            logger.error("Couldn't initialize the parser",e);
        }  catch (Exception e){
            logger.error("Something unforeseen has happened..",e);
        }
    }



    static class Builder {
        private WMBMonitorTask task = new WMBMonitorTask();

        Builder shutdownHandler(WMBMonitorTaskShutdownHandler shutdownHandler) {
            task.shutdownHandler = shutdownHandler;
            return this;
        }

        Builder metricPrefix(String metricPrefix) {
            task.metricPrefix = metricPrefix;
            return this;
        }

        Builder metricWriter(MetricWriteHelper metricWriter) {
            task.metricWriter = metricWriter;
            return this;
        }

        Builder manager(Map manager){
            task.queueManagerConfig = manager;
            return this;
        }

        WMBMonitorTask build() {
            return task;
        }
    }

    public void setInitialDelayInMinutes (int delay) {
        initialDelayInMinutes = delay;
    }
}
