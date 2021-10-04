# poc-kafka-beam-python
Create a data processing pipeline that listens to tweets and write them
to an apache kafka topic, consumes and processes them through an apache beam pipeline
and outputs the results to a second kafka topic. The goal is to track real time trends
in specific locations via a dashboard that streams from a kafka topic via a FastAPI endpoint 
Work in progress, the main focus is realizing a fully working solution


Issues: spent way too much time debugging beam streaming via a kfka consumer to
discover that it is not supported for the local runner. I wrote custom kafka 
consumer/producer for beam by subclassing `apache_beam.PTransform`, that got me to consume the
kafka topic messages with no java errors but the windowing function for
aggregating unbounded data did not work https://beam.apache.org/documentation/sdks/python-streaming/#unsupported-features

# TODO
- [ ] Add fast API and dashboard
- [ ] write a RADME.md with instructions
- [ ] Refactor the code to be production ready
- [ ] Add logging and exception handling
- [ ] Dockerize 
- [ ] Add instructions to run serverless on GCP
- [ ] Add gRPC API
