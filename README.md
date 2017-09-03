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

Note that this is likely to lead to data loss in processing, or reprocessing depending on the direction of movement.
