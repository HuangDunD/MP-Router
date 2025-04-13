#include "config.h"

int ComputeNodeCount = 4;
uint64_t ATTEMPTED_NUM = 10000000;
double CrossNodeAccessRatio = 0.2;
int REGION_SIZE = 1000;
std::string conninfo = "host=localhost port=15432 dbname=template1 user=gpadmin password=gpadmin";