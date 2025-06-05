#!/usr/bin/env bash

if [ $# -lt 1 ] ; then
    echo "usage: $(basename $0) PROPS [OPT VAL [...]]" >&2
    exit 2
fi

PROPS="$1"
shift
if [ ! -f "${PROPS}" ] ; then
    echo "${PROPS}: no such file or directory" >&2
    exit 1
fi

#DB="$(grep '^db=' $PROPS | sed -e 's/^db=//')"

Benchmark_Type="$(grep '^benchmarkType=' $PROPS | sed -e 's/^benchmarkType=//')"

if [ "$Benchmark_Type" = "tpcc" ]; then
    echo "Benchmark Type is tpcc"

    BEFORE_LOAD="tableCreates extraCommandsBeforeLoad storedProcedureCreates"

    AFTER_LOAD="indexCreates foreignKeys buildFinish"

    for step in ${BEFORE_LOAD} ; do
        ./runSQL.sh "${PROPS}" $step &
        PID=$!
        while true ; do
    	kill -0 $PID 2>/dev/null || break
    	sleep 1
        done
        wait $PID
        rc=$?
        [ $rc -eq 0 ] || exit $rc
    done

    ./runLoader.sh "${PROPS}" $* &
    PID=$!
    while true ; do
        kill -0 $PID 2>/dev/null || break
        sleep 1
    done
    wait $PID
    rc=$?
    [ $rc -eq 0 ] || exit $rc

    for step in ${AFTER_LOAD} ; do
        ./runSQL.sh "${PROPS}" $step &
        PID=$!
        while true ; do
    	kill -0 $PID 2>/dev/null || break
    	sleep 1
        done
        wait $PID
        rc=$?
        [ $rc -eq 0 ] || exit $rc
    done

elif [ "$Benchmark_Type" = "smallbank" ]; then
    echo "Benchmark Type is smallbank"
    BEFORE_LOAD="./sql.smallbank/tableCreates.sql"

      for step in ${BEFORE_LOAD} ; do
          ./runSQL.sh "${PROPS}" $step &
          PID=$!
          while true ; do
        kill -0 $PID 2>/dev/null || break
        sleep 1
          done
          wait $PID
          rc=$?
          [ $rc -eq 0 ] || exit $rc
      done

    smallbankOPTS="-Dprop=${PROPS}"
    smallbankOPTS="${smallbankOPTS} -Djava.security.egd=file:/dev/./urandom"

    java -cp "./:../MockBenchSQL.jar:../lib/*" $smallbankOPTS mock.bench.Tpcc.DataLoad.SmallBankLoader
#    echo "java -cp "./:../MockBenchSQL.jar:../lib/*" $smallbankOPTS mock.bench.Tpcc.DataLoad.SmallBankLoader"
else
    echo "Benchmark Type is not tpcc or smallbank, exiting"
    exit 1
fi

