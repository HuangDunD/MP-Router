#pragma once
#include <string>

class Metis
{
private:
    /* data */
public:
    Metis(/* args */){};
    ~Metis(){};
    void metis_partition_graph(const std::string& output_path_);
};

