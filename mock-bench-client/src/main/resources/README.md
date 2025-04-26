# load data
./runDatabaseBuild.sh my.properties
# delete data
./runDatabaseDestroy.sh my.properties
# run tests
./runBenchmark.sh my.properties
# generate report
./generateReport.py -t report_simple.html -r your_results_dir
