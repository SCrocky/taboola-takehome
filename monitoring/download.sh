#!/bin/bash
# Downloads the Prometheus JMX Exporter Java agent into jmx_exporter/.
wget -O jmx_exporter/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar
