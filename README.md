# rltrace

Python module for writing trace logs to multiple destinations

Logging to console support is attached by default & has an elastic DB handler that can optionally be created and
attached.

## Project Structure

### trace/Elastic

An example set of Kubernetes yaml config files that can be used to create a very light weight and stand-alone
Elastic/Kibana pair. This is tested only with Docker Desktop on Windows, but the Kibana YAML is standard and not Windows
specific. The only change you will need ot make is to update
the [elastic-data-persistent-volume.yaml](https://github.com/parrisma/trace/blob/master/elastic/k8s-elastic/elastic-data-persistent-volume.yaml)
file. Here you will need to update <code>path</code> with a path on your local host where the elastic Db is to be
created.

### trace/test

A full set of tests for teh basic trace and extended elastic trace module. For these tests to run you will need an
elastic DB server running. The tests create a test index which is cleaned away at the end. You will need to edit the
Elastic <code>host, port, username and pwd</code> in the <code>setUp</code> function of the
file[TestElasticTrace.py](https://github.com/parrisma/trace/blob/master/test/TestElasticTrace.py)

### trace/rltrace

The rltrace module, where the core trace module also redirects TensorFlow logs by default so TF logs output is captured
in all the attached handlers.

## Use

### Base use case.

The trace messages for all handlers are of the form

```
time stamp
session_uuid
log level
message

e.g.

2022-09-12T11:08:43.+0800 — 3b11870d487b42029b1c17277f9a41e7 - INFO — - - - - - - Hello World ! - - - - - -
```

At construnction a session_uuid is allocated, but if you wish to partition activity e.g. different training runs of a
neural network, you can force a new session uuid to be allocated

```python
from rltrace import Trace, LogLevel
trace: Trace = Trace(log_level=LogLevel.debug)
trace.new_session()
```

Create a Trace object and use to log all traffic you want captured by the handlers. The intended use is for all logging.

```python
import numpy as np
from rltrace import Trace, LogLevel

trace: Trace = Trace(log_level=LogLevel.debug)
trace().info(f'Test Message {np.random.rand()}')
```

By default, the additional handlers are not attached, so logging is to the console only. The additional handlers can be
attached as follows.

### Elastic

This assumes you have a running elastic search engine. See the
example [Kubernetes YAML](https://github.com/parrisma/trace/tree/master/elastic/k8s-elastic) config for how you can run
one locally. Where the index defintion to store the trace logs can be found
in [elastic-log-index.json](https://raw.githubusercontent.com/parrisma/trace/master/elastic/k8s-elastic/elastic-log-index.json)

```python
import json
from rltrace import Trace, LogLevel
from elastic import ESUtil, ElasticHandler

es_connection = ESUtil.get_connection(hostname='localhost',
                                      port_id=port_id,
                                      elastic_user='elastic',
                                      elastic_password='changeme')

index_mappings = json.load(open('resources/elastic-log-index.json'))
res = ESUtil.create_index_from_json(es=es_connection,
                                    idx_name=handler_index_name,
                                    mappings_as_json=index_mappings)

if not res:
    raise Exception(f'failed to create index {handler_index_name} for testing elastic logging')

elastic_handler = ElasticHandler(es=es_connection,
                                 trace_log_index_name=handler_index_name)
trace: Trace = Trace(log_level=LogLevel.debug)
trace.enable_handler(elastic_handler)  # Message now go to console & the elastic index created above
```
