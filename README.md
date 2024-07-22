# Background Transport

A background data transport.

## Installation

The basic library is installable via `pip`

```shell
pip install bgtransport
```
But you will need two additional things to make things work: a transport method and a serialization method.


## Example

```python
from confluent_kafka import Producer
from bgtransport.transport.kafka import KafkaTransport, KeyGetter
from ds_serialization.json import dumps
from bgtransport import create_task


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


kafka_client = Producer({'bootstrap.servers': 'mybroker1,mybroker2'})
produce_kwargs = {"callback": delivery_report}
transport = KafkaTransport(
    serializer=dumps,
    kafka_client=kafka_client,
    topic="foobarauditor",
    key_generator=KeyGetter("uid"),
    flush_timeout=0.5,
    produce_kwargs=produce_kwargs,
)

create_task("auditor", transport)
```

```python
from bgtransport import get_task
from .stuff import do_something

auditor = get_task("auditor")


def route(request):
    auditor.new_record()
    do_something(request)
    auditor.send_record()
    return 200
```

```python
from bgtransport import get_task

auditor = get_task("auditor")


def do_something(request):
    auditor.update(key=value, key2=value2, key3=value3)
```

```python
from starlette_context import request_cycle_context
from fastapi import FastAPI, Depends


kafka_client = Producer({'bootstrap.servers': 'mybroker1,mybroker2'})
produce_kwargs = {"callback": delivery_report}
transport = KafkaTransport(
    serializer=dumps,
    kafka_client=kafka_client,
    topic="foobarauditor",
    key_generator=KeyGetter("uid"),
    flush_timeout=0.5,
    produce_kwargs=produce_kwargs,
)

async def setup_audit_task():
    """
    This sets up the audit task in each request.
    """
    create_task("auditor", transport)
    
    with request_cycle_context({}):
        yield

# use it as Depends across the whole FastAPI app
app = FastAPI(dependencies=[Depends(my_context_dependency)])
```
