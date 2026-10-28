// Read-only offline rule-score comparison; does not route or execute transactions.
// Build separately with libpqxx; SERON_PROBE_CONNINFO supplies connection details.
// Optional argv[1]: lines "table.column boundary0 ... boundaryN" for frozen rules.
#include "seron_physical_sampling.h"
#include "seron_range_rules.h"
#include "seron_range_workload.h"
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>

static double empirical(const std::vector<seron_tv::PhysicalValue>& rows,
                        const std::vector<uint64_t>& bounds) {
    std::map<uint64_t,std::vector<uint64_t>> pages;
    for (auto row:rows) {
        auto& counts=pages[row.page];
        if (counts.empty()) counts.resize(bounds.size()-1);
        ++counts[std::upper_bound(bounds.begin()+1,bounds.end()-1,row.value)-(bounds.begin()+1)];
    }
    long double total=0,same=0;
    for (const auto& page:pages) {
        uint64_t n=0;
        for(auto x:page.second) {n+=x;same+=static_cast<long double>(x)*(x?x-1:0);}
        total+=static_cast<long double>(n)*(n?n-1:0);
    }
    return total ? double(1-same/total) : -1;
}

int main(int argc,char** argv) {
    try {
        const char* conninfo=std::getenv("SERON_PROBE_CONNINFO");
        if (!conninfo) throw std::runtime_error("Set SERON_PROBE_CONNINFO");
        std::map<std::string,std::vector<uint64_t>> frozen;
        if(argc>1) {
            std::ifstream input(argv[1]);
            if(!input) throw std::runtime_error("Cannot open boundary file");
            std::string line;
            while(std::getline(input,line)) {
                std::istringstream tokens(line); std::string name; uint64_t b;
                if(!(tokens>>name))continue;
                while(tokens>>b)frozen[name].push_back(b);
            }
        }
        pqxx::connection connection(conninfo);
        pqxx::work db(connection);
        db.exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
        db.exec("SET LOCAL statement_timeout='120s'");
        // Same benchmark dimensions as the current 200-warehouse experiments.
        const auto specs=seron_range::specs(10000000,200,3000,10000);
        std::map<std::string,std::vector<size_t>> tables;
        for(size_t i=seron_range::WarehouseId;i<seron_range::Count;++i)
            tables[specs[i].table].push_back(i);
        std::cout<<std::setprecision(10);
        for(const auto& table:tables) {
            std::vector<std::string> names;std::vector<uint64_t> domains;
            for(auto id:table.second){names.push_back(specs[id].column);domains.push_back(specs[id].max_value);}
            auto samples=seron_tv::sample_relation(db,"public."+db.quote_name(table.first),names,domains,std::cout);
            for(size_t j=0;j<names.size();++j) {
                const std::string name=table.first+"."+names[j];
                const auto stats=seron_tv::statistics(samples.columns[j],domains[j],samples.complete);
                const auto eq=SeronRangeRules(domains[j],4,64).snapshot()->boundaries;
                auto report=[&](const std::string& phase,const std::vector<uint64_t>& bounds) {
                    if(bounds.size()!=5 || bounds.front()!=1 || bounds.back()!=domains[j]+1 ||
                       !std::is_sorted(bounds.begin(),bounds.end()))
                        throw std::runtime_error("Boundary domain mismatch for "+name);
                    std::cout<<"PROBE "<<name<<" phase="<<phase
                        <<" rows="<<stats.rows<<" pages="<<stats.pages<<" full_scan="<<samples.complete
                        <<" mean_difference="<<stats.mean_difference<<" normalized_step="<<stats.normalized_step
                        <<" m="<<stats.records_per_page<<" tv_rate="<<seron_tv::estimate(samples.columns[j],stats,bounds)
                        <<" empirical_rate="<<empirical(samples.columns[j],bounds)<<'\n';
                };
                report("equal-width",eq);
                if(frozen.count(name))report("previous-frozen",frozen.at(name));
            }
        }
        db.commit();
    }catch(const std::exception& e){std::cerr<<"Probe failed: "<<e.what()<<'\n';return 1;}
}
