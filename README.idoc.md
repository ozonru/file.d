# Overview
`file.d` is a blazing fast tool for building data pipelines: read, process and output events. Primarily developed to read from files, but also supports numerous input/action/output plugins. 

> ⚠ However we use it in production `it's still less than v1.0.0`. Please, test your pipelines carefully on dev/stage environments.  

## Motivation
Well, we already have a number of similar tools: vector, filebeat, logstash, fluend-d, fluent-bit, etc.

Performance tests states that best ones achieve around **100MB/sec** throughput. 
Guys, its 2020 now. HDDs and NICs can handle throughput of a **few GB/sec** and CPUs processes **dozens of GB/sec**. Are you sure 100MB/sec is what we deserve? Are you sure 100MB/sec is fast?

## Main features
* More than 10x faster compared to the similar tools
* Predictable: it uses pooling, so memory consumption is limited 
* Container / cloud / kubernetes native
* Simply configurable with YAML
* Prometheus-friendly: transform your events into metrics on any pipeline stage
* Well tested and used in production to collect logs from kubernetes cluster with 4500+ total CPU cores
* Reliable: doesn't loose data due to commitment mechanism

## Performance
On MacBook Pro 2017 with two physical cores `file.d` can achieve throughput:
* 1.7GB/s in `files > devnull` case
* 1.0GB/s in `files > json decode > devnull` case

Throughput on production server to be filled.  

## Plugins

**Input**: @global-contents-table-plugin-input|links

**Action**: @global-contents-table-plugin-action|links

**Output**: @global-contents-table-plugin-output|links

## What's next
* [Quick start](/docs/quick-start.md)
* [Installation](/docs/installation.md)
* [Examples](/docs/examples.md)
* [Configuring](/docs/configuring.md)
* [Architecture](/docs/architecture.md)
* [Contributing](/docs/contributing.md)