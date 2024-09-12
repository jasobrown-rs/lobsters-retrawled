#!/bin/bash

################################
# A sample script to execute the lobsters benchmark
# multiple times with different configuration for each run.
#
# Assumes you already have Readyset running _somewhere_.


set -euo pipefail

################################
# configuration 

# required variables - these are defaults for a local deployment.
db_address="127.0.0.1"
db_port="3306"
db_name="lobsters"
db_user="root"
db_pw="noria"

rs_address="127.0.01"
rs_port="3307"

# url of a prometheus push gateway where the benchmark can publish metrics.
# set to empty to skip this.
push_gateway_url="http://localhost:9091/metrics/job/lobsters-benchmark"


# benchmark variables
scale=1
in_flight=100
run_for_sec=10

# values are [noria,original]
queries="original"

# random convience variables
# time between runs
sleep_sec=60

driver_bin="target/release/lobsters"
upstream_url="mysql://${db_user}:${db_pw}@${db_address}:${db_port}/${db_name}"
rs_url="mysql://${db_user}:${db_pw}@${rs_address}:${rs_port}/${db_name}"

log_dir=./$(date +%s)
mkdir $log_dir

# run the benchmark
#
# params:
# $1 - log file to write out to
# $2 - should the benchmark set up the database (generating data ...) {true|false}
# $3 - execute the benchmark against the database or readyset {database|readyset}
run_benchmark() {
    # run driver the foreground
    local log_file=$1
    local should_prime=$2
    local target=$3

    # by default, skip priming data into the database
    local prime=""
    if [ "$should_prime" == "true" ]; then
        prime="--prime"
    fi

    local url=$upstream_url
    if [ "$target" == "readyset" ]; then
        url=$rs_url
    fi

    local prometheus=""
    if [ ! -z "$push_gateway_url" ]; then
        prometheus="----prometheus-push-gateway ${push_gateway_url}"
    fi

    $driver_bin $prime --dbn $url \
                --runtime ${run_for_sec} \
                --scale ${scale} \
                --in-flight ${in_flight} \
                --queries ${queries} \
                > $log_dir/driver_out.log

    if [ $? -ne 0 ]; then
        echo "**** The benchmark failed. ****"
        exit 1
    fi
}


cache_queries() {
    # mysql cli to connect to readyset
    mysql_cmd="MYSQL_PWD=${db_pw} mysql --host=${rs_address} --port=${rs_port} --user=${db_user} --database=${db_name}"

    # ask readyset for all the proxied queries
    proxied_queries="${mysql_cmd} -e \"show proxied queries\""
    eval "$proxied_queries" > /tmp/rs-queries.txt

    # pick out the query_ids of the supported queries
    grep -i select /tmp/rs-queries.txt | grep yes | awk '{print $1}' > /tmp/rs-ids.txt

    # now, create caches for those queries
    for i in $(cat /tmp/rs-ids.txt); do
        echo "*** $i ***"
        cache_query="${mysql_cmd} -e \"create cache from ${i}\""
        eval "$cache_query"
    done

}

################################
################################
################################
## start the main event!


################################
## step 0 - ensure we have a release build of the benchmark app
echo "** running step 0 **"
cargo --locked build --release


# ################################
# ## step 1 - get a baseline of of simply reading from the upstream database.
# ## this take a while as we load up the data into the upstream here
echo "** running step 1 **"
run_benchmark "${log_dir}/upstream.log" "true" "database"
sleep $sleep_sec


# ################################
# ## step 2 - get a baseline of readyset as a proxy to the upstream database.
echo "** running step 2 **"
run_benchmark "${log_dir}/readyset-proxy.log" "false" "readyset"
sleep $sleep_sec


################################
## step 3 - create caches for all the supported queries.
echo "** running step 3 **"
cache_queries

# ################################
# ## step 4 - execute with readyset cacheing.
echo "** running step 3 **"
run_benchmark "${log_dir}/readyset-cache.log" "false" "readyset"


