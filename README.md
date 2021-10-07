# poc-kafka-beam-python
Create a data processing pipeline that listens to tweets and writes them
to an apache kafka topic, consumes and processes them through an apache beam pipeline
and outputs the results to a second kafka topic. The goal is to count the word frequency given a twitter search 
criteria via a dashboard that streams from a kafka topic via a FastAPI endpoint 
Work in progress, the main focus is realizing a fully working solution rather than perfecting
a single component in the pipeline


Issues: Beam local runner doesn't support KafKa IO for a streaming pipeline. I wrote custom kafka 
consumer/producer for beam by subclassing `apache_beam.PTransform`, that got me to consume the
kafka topic messages with no java errors but the windowing function for
aggregating unbounded data did not work https://beam.apache.org/documentation/sdks/python-streaming/#unsupported-features

# TODO
- [x] Add fast API trends topic endpoint
- [ ] Add a UI dashboard
- [ ] write a RADME.md with docker instructions
- [ ] Add unit and integration tests
- [ ] Add logging and exception handling
- [x] Dockerize 
- [ ] Add instructions to run serverless on GCP
- [ ] Add gRPC API
- [ ] Fine tune kafka topic partitions and keys
