#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

void consume_messages(const char* topic_name);