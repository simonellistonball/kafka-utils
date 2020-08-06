# kafka-utils
A set of utilities for doing things you shouldn't do with broken Kakfa brokers


## ConsumerGroupOffestTweaker

Manually change (force commit) a kafka managed offset for a given consumer group, topic and partition. 

```
java -cp kafka-utils-0.0.1-SNAPSHOT.jar com.simonellistonball.kafka.utils.ConsumerGroupOffestTweaker -b <BOOTSTRAP_SERVERS> -g <GROUP> -o <OPTION> -p <PARTITION> -t <TOPIC> 

usage: ConsumerGroupOffestTweaker -b <BOOTSTRAP_SERVERS> -g <GROUP> -o <OPTION> -p <PARTITION> -t <TOPIC> 
    -b,--bootstrap-servers <BOOTSTRAP_SERVERS>   Comma separated list of
                                         		brokers to bootstrap from
	-g,--group <GROUP>                           Consumer group to operate on
	-o,--offest <OPTION>                         Commit this offset
	-p,--partition <PARTITION>                   Partition to change offset on
	-t,--topic <TOPIC>                           Topic to operate on
	
```

Note that this is likely to lead to data loss in processing, or re-processing depending on the direction of movement.

You probably won't need this once <https://issues.apache.org/jira/browse/KAFKA-5181> has been completed. <https://cwiki.apache.org/confluence/display/KAFKA/KIP-122%3A+Add+Reset+Consumer+Group+Offsets+tooling> and <https://issues.apache.org/jira/browse/KAFKA-4743> also cover similar use cases.  



## TopicOffsetRange

Prints a table of all the earliest and latest available offets for a given topic.

```
java -cp kafka-utils-0.0.2-SNAPSHOT.jar com.simonellistonball.kafka.utils.TopicOffsetRange -b <BOOTSTRAP_SERVERS> -t <TOPIC> -f <PROPERTIES_FILE>

usage: ConsumerGroupOffestTweaker -b <BOOTSTRAP_SERVERS> -g <GROUP> -o <OPTION> -p <PARTITION> -t <TOPIC> 
  -b,--bootstrap-servers <BOOTSTRAP_SERVERS>  Comma separated list of
                                              brokers to bootstrap from
	-t,--topic <TOPIC>                          Topic to operate on
	-f,--properties <PROPERTIES_FILE>           File containing Kafka Consumer properties
	
```
