# lobsters benchmark 2.0

This project is fork of the [`lobsters` benchmark](https://github.com/mit-pdos/noria/tree/master/applications/lobsters) that was part of the original [Noria thesis](https://jon.thesquareplanet.com/papers/phd-thesis.pdf) and [ODSI paper](https://www.usenix.org/conference/osdi18/presentation/gjengset).

This version modernized the code to compile with newer versions of the rust compiler toolchain, and simplified some of the original's async-related code. Otherwise, it's premise remains unchanged, and I encourage you to read the above papers for full details of how the benchmark works.

This application can be run against either a MySQL database or Readyset (fronting a MySQL database). There's desire/plans to support PostgreSQL, as well, but it's not implemented yet.

## Building
Make sure to build with the `--release` flag:
```
cargo --locked build --release
```

If you are compiling your own Readyset, it should be implemented with `--release`, as well, or else perf will be much lower:
```
cd <readyset_dir>
cargo --locked build --release --bin readyset
```

## Running
Notable cli flags include:
- `dbn` - The address of the database or Readyset instance to connect to; for example, `mysql://root:noria@127.0.0.1:3307/noria`.
- `runtime` - Duration in seconds to run the benchmark.
- `in-flight` - Number of allowed Concurrent requests. Be careful tuning this too high, as this will be the max number of open connections to the upstream database/Readyset. (Readyset can handle a large number of connections, but upstream databases strguggle, especially Postgres).
- `prime` - Set up the tables and generate data to be used.
- `scale` - Load factor for workload. *If you change this value between runs of the application, you will need to "prime" the data set again*. 

Optional flags:
- `prometheus-push-gateway` - Optional HTTP URL where prometheus metrics can be sent. Metrics include page load times.

### Sample execution
A sample cli execution might look like this:
```
cargo --locked run --release -- --dbn mysql://root:noria@127.0.0.1:3307/testdb \
    --prometheus-push-gateway http://localhost:9091/metrics/job/lobsters-benchmark \
    --runtime 60 \
    --scale 256 \
    --in-flight 512 \
    --prime
```

In this example:
- I will `prime` the data set.
- I am setting a very reasonable scale, `256`, along with a decent `in-flight` size of `512`.
- I am hosting a container running prometheus [pushgateway](https://github.com/prometheus/pushgateway) at port `9091`, and pushing metrics from the benchmark app there.

### Batch script

There's an example shell script of how to orchestrate an end-to-end run which compares testing against the upstream data base versus Readyset.

## Reporting
At the end of each run, the benchmark will emit per-page metrics. For example:

*jasobrown to fill this in*

Alternatively, there is a sample (read: naive) [grafana dashboard](./dashboards/lobsters.json) in this repo you can use a point of departure for graphing the counts and latency histograms.

