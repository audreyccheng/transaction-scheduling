# DjinnDB

This repository contains code implementing DjinnDB, a transactional scheduling system. DjinnDB searches for and executes fast schedules for transactional workloads.

This repository contains:
1. DjinnDB, which modifies [RocksDB](https://rocksdb.org/) to add a classifier that predicts hot key conflict patterns, our SMF scheduler, and a schedule-first concurrency control protocol, MVSchedO
2. Benchmarking code for the paper with benchmarks based on [OLTPBench](https://github.com/oltpbenchmark/oltpbench) and [TAOBench](https://taobench.org/)
3. Makespan simulator that takes transaction traces as input and makespans for a specified isolation level and concurrency control protocol
4. JSS formulation of transaction scheduling as an appendix to the paper

This repository is structured as follows:
- /benchmarks - the application benchmarks tested on DjinnDB
- /rocksdb - the modified version of RocksDB supporting transactional scheduling
- /simulator - the makespan simulator

Prerequisites:
- mvn 3.8.5
- build-essential
- Java 17
- C++17 required (GCC >= 7, Clang >= 5)

Note: if running on EC2 or other cloud providers, make sure security groups / firewalls allow all traffic.

## DjinnDB
To run benchmarks against DjinnDB, start DjinnDB and the benchmark code on separate machines. The benchmark starts up worker threads that send transactions over the network to the machine hosting DjinnDB. These requests are received by a database proxy, which then redirects the transactions to DjinnDB. We mainly modify /rocksdb/utilities/transactions and /rocksdb/java since the benchmark is in Java.

To run a benchmark:

1. `cd benchmarks`

2. `mvn clean install`

3. `mvn compile assembly:single`

4. Configure benchmark client: `____ExpConfig.json` (examples available in /benchmarks/configs)

    - `exp_length`: experiment length in seconds

    - `ramp_up`: warm-up time in seconds

    - `must_load_keys`: whether loading phase should run

    - `nb_loader_threads`: number of threads during loading phase

    - `n_worker_threads`: number of threads available to the benchmark

    - `nb_client_threads`: number of threads available to generate transactions

    - `n_receiver_net_threads`: number of threads to receive network messages

    - `n_sender_net_threads`: number of threads to send network messages

    - `node_ip_address`: IP address of benchmark machine

    - `proxy_ip_address`: IP addres of DjinnDB machine

    - `proxy_listening_port`: post on which DjinnDB machine is listening for client benchmark requests

    - `must_schedule`: turn on / off scheduler

5. To run loader and benchmark client: `java -jar ___.jar ___ExpConfig.json`

Note: to build a different jar, edit `pom.xml` plugin.

To build DjinnDB jar:

1. `mkdir build; cd build`

2. `cmake ..`

3. `cd ..`

3. `make DEBUG_LEVEL=0 rocksdbjava`

To run DjinnDB and database proxy, add DjinnDB build file to jar of database proxy:

1. `cd benchmarks`

2. `mvn install:install-file  -Dfile=DjinnDB jar> -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=1 -Dpackaging=jar -DgeneratePom=true`

3. `mvn compile assembly:single`

4. Configure database proxy: `____ExpConfig.json` (example available in /benchmarks/configs)

    - `n_worker_threads`: number of threads available to the proxy

    - `nb_client_threads`: number of threads available to send requests over the network

    - `n_receiver_net_threads`: number of threads to receive network messages

    - `n_sender_net_threads`: number of threads to send network messages

    - `node_ip_address`: IP address of benchmark machine

    - `proxy_ip_address`: IP addres of DjinnDB machine

    - `proxy_listening_port`: post on which DjinnDB machine is listening for client benchmark requests

    - `useproxy`: whether or not proxy should be used

    -  `delete_db`: delete contents of DjinnDB

5. To start DjinnDB and database proxy: `java -jar ___.jar ___ExpConfig.json`

## Makespan simulator

We implement a makespan simulator in Python to generate performance distributions of schedule spaces. Our simulator supports different concurrency control protocols (MVTSO, 2PL, OCC) and various scheduling policies, including SMF, JSS techniques, and transaction scheduling algorithms.

To run the simulator:

1. `parse_cc.py` contains the majority of the simulation code. To specifiy a concurrency control protocol:

    - `opt`: MVTSO

    - `occ`: OCC

    -  `lock`: 2PL

2. Run with `python parse_cc.py -b <batch size> -p <print workload>`
