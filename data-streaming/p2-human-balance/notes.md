# Changes
- submit-event-kafkajoin.cmd -> version updated from 3.0.0 to -> 3.1.1 -> fixed the error related to StreamingQueryException 
- developed in windows, used cmd commands. commands changed according to the folder structure
- used local docker env for development. bootstrap Kafka server is located at `kafka:19092`
- `# %%` symbols in python files are for Jupiter notebook. `PYSPARK_SUBMIT_ARGS` is also used for local tests
- spark-master.log generation requires `>& .` flag which doesn't work on windows. Equivalent operator `| Out-File` also didn't generated the expected results. So I manually copied the logs from `docker logs  data_streaming_p2_spark_1` into the folder 
- errors in log files are from termination of processes. waited a bit on one of them and created more errors than usual.
- `stedi-application` subfolder and `application.conf` exists in udacity workspace but these folder and files didin't exist in the repo `https://github.com/udacity/nd029-c2-apache-spark-and-spark-streaming-starter/tree/master/project/starter` so I didn't submit them.


# Run
- submit-event-kafkastreaming.cmd
- submit-event-kafkajoin.cmd
- docker logs -f  data_streaming_p2_stedi_1
- http://localhost:4567/risk-graph.html