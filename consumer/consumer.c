#include "consumer.h"

void consume_messages(const char* topic_name) {
    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // Set group.id for consumer group
    if (rd_kafka_conf_set(conf, "group.id", "my-group", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% Failed to set group.id: %s\n", errstr);
        exit(1);
    }

    // Enable auto commit
    rd_kafka_conf_set(conf, "enable.auto.commit", "true", NULL, 0);

    // Create Kafka consumer instance
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create consumer: %s\n", errstr);
        exit(1);
    }

    // Subscribe to the topic
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic_name, -1);
    rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    printf("Consuming messages from topic: %s\n", topic_name);

    // Poll for new messages
    while (1) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (!rkmessage) continue;

        if (rkmessage->err) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                // Reached end of partition
            } else {
                fprintf(stderr, "%% Consumer error: %s\n", rd_kafka_message_errstr(rkmessage));
            }
        } else {
            printf("Received message: %.*s\n",
                   (int)rkmessage->len,
                   (const char*)rkmessage->payload);
        }

        rd_kafka_message_destroy(rkmessage);
    }

    // Clean shutdown
    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
}