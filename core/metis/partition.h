#pragma once
#include <string>
#include <metis.h>
#include <partition.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <memory>
#include <set>
#include <sstream>

class Metis
{
private:
    /* data */
public:
    Metis(/* args */){};
    ~Metis(){};
    void metis_partition_graph(const std::string& input_path, const std::string& output_path);
    void process_transactions_to_graph(const std::string& input_path, const std::string& output_path);
};

