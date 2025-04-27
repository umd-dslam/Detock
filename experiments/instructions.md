Following is the instructions for running the experiments that generate the results in the paper [Detock: High Performance Multi-region Transactions at Scale](https://doi.org/10.1145/3589293).

# Set up the AWS environment

## Simulated network delay deployment
This deployment is used for the microbenchmark experiments. In this deployment, the system resides in a single AWS region.

1. Create 32 EC2 instances of type r5.4xlarge (for the servers) and 8 EC2 instances of type r5.2xlarge (for the clients). Make sure they run Ubuntu and the disk size of each instance is at least 20 GB.
1. Create a security group that allows inbound traffic to ports 2020-2025 and attach the security group to all the instances.
1. Install Docker on all the instances. You can either do it manually or run the following command. Make sure that you can interact with AWS via the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html), and install the Python dependencies in `tools/requirements.txt` (e.g., `pip3 install -r tools/requirements.txt`).

    ```bash
    python3 tools/aws.py docker --region us-east-1
    ```

1. Update the `servers_public`, `servers_private`, and `clients` in `experiments/settings.json` with the public and private IP addresses of the servers and the public IP addresses of the clients.

1. Run the experiment in dry run mode to generate a configuration to be used for the next step.

    ```bash
    python3 tools/run_experiment.py ycsb --settings experiments/settings.json -n ycsb --dry-run
    ```

1. Generate the script to emulate latency between the servers.
    ```bash
    python3 tools/admin.py gen_netem /tmp/baseline.conf experiments/latency.csv
    ```

1. Run the following commands on every server. You can use [System Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/run-command.html) to do so.
    ```bash
    sudo /home/ubuntu/netem.sh
    sudo sysctl -w net.core.rmem_max=10485760
    sudo sysctl -w net.core.wmem_max=10485760
    sudo apt-get install -y -q chrony
    sudo sed -i '1s/^/server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4\n/' /etc/chrony/chrony.conf
    sudo /etc/init.d/chrony restart
    ```

## Full deployment

This deployment is used for all other experiments. In this deployment, the system is deployed 8 regions:

- us-east-1
- us-east-2
- eu-west-1
- eu-west-2
- ap-northeast-1
- ap-northeast-2
- ap-southeast-1
- ap-southeast-2

*The scalability and CockroachDB experiments are deployed in a different set of regions.*

In each region:

1. Create [VPC peering connections](https://docs.aws.amazon.com/vpc/latest/peering/what-is-vpc-peering.html) to all other regions.
1. Create a security group that allows inbound traffic to ports 2020-2025.
1. Launch 4 EC2 instances of type r5.4xlarge (for the servers) and 1 EC2 instance of type r5.2xlarge (for the clients).
    - Make sure they run Ubuntu and the disk size of each instance is at least 20 GB.
    - Attach them with the security group above.
1. Install Docker on all the instances. You can either do it manually or run the following command. Make sure that you can interact with AWS via the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html), and install the Python dependencies in `tools/requirements.txt` (e.g., `pip3 install -r tools/requirements.txt`).

    ```bash
    python3 tools/aws.py docker --region us-east-1 us-east-2 eu-west-1 eu-west-2 ap-northeast-1 ap-northeast-2 ap-southeast-1 ap-southeast-2
    ```

1. Update the `servers_public`, `servers_private`, and `clients` in `experiments/settings.json` with the public and private IP addresses of the servers and the public IP addresses of the clients.

1. Run the following commands on every server. You can use [System Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/run-command.html) to do so.
    ```bash
    sudo sysctl -w net.core.rmem_max=10485760
    sudo sysctl -w net.core.wmem_max=10485760
    sudo apt-get install -y -q chrony
    sudo sed -i '1s/^/server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4\n/' /etc/chrony/chrony.conf
    sudo /etc/init.d/chrony restart
    ```

# Run the experiments

In the following instructions, we assume that the output directory is `~/data/detock`. You can change it to any directory you want.

## Observability

Throughout these experiments, you can look at the logs of the nodes with the following command:

```bash
python3 tools/admin.py logs /tmp/baseline.conf -rp <region-id> <partition-id> -f
```

with `<region-id>` and `<partition-id>` indicating the region and partition of the node you want to look at. To look at the logs of the clients, add `--client` to that command.

## Microbenchmark

### Throughput

The following commands collect throughput results for Detock, Detock (w/o opportunistic ordering), and SLOG.

```bash
python3 tools/run_experiment.py ycsb --settings experiments/settings.json --out-dir ~/data/detock --tag hot mp mh
```

```bash
python3 tools/run_experiment.py ycsb2 --settings experiments/settings.json -n ycsb --out-dir ~/data/detock --tag hot mp mh
```

The following commands collect throughput results for Calvin and Janus, respectively. Note that these commands use a different settings file (i.e. `settings-calvin.json` and `settings-janus.json`), so make sure to update the `servers_public`, `servers_private`, and `clients` in those files.

```bash
python3 tools/run_experiment.py ycsb --settings experiments/settings-calvin.json --out-dir ~/data/detock --tag hot mp mh
```

```bash
python3 tools/run_experiment.py ycsb --settings experiments/settings-janus.json --out-dir ~/data/detock --tag hot mp mh
```

### Latency

```bash
python3 tools/run_experiment.py ycsb-latency --settings experiments/settings.json -n ycsb --out-dir ~/data/detock --tag hot mp mh
```

```bash
python3 tools/run_experiment.py ycsb-latency --settings experiments/settings-calvin.json -n ycsb --out-dir ~/data/detock --tag hot mp mh
```

```bash
python3 tools/run_experiment.py ycsb-latency --settings experiments/settings-janus.json -n ycsb --out-dir ~/data/detock --tag hot mp mh
```

### Aymmetric delay

```bash
python3 tools/run_experiment.py ycsb-asym --config-dir config --out-dir ~/data/detock -n ycsb-asym
```

### Network jitter

```bash
python3 tools/run_experiment.py ycsb-jitter --config-dir config --out-dir ~/data/detock -n ycsb-jitter
```

## TPC-C

### Throughput

```bash
python3 tools/run_experiment.py tpcc --settings experiments/settings.json --out-dir ~/data/detock/
```

```bash
python3 tools/run_experiment.py tpcc --settings experiments/settings-calvin.json --out-dir ~/data/detock/
```

```bash
python3 tools/run_experiment.py tpcc --settings experiments/settings-janus.json --out-dir ~/data/detock/
```

## Scalability

This experiment is deployed in the following regions:
- us-east-2
- eu-west-1
- ap-northeast-1

Use the "Full deployment" instructions above to deploy the system in these regions.

The following commands use a different settings file (`settings-scale.json`), so make sure to update the `servers_public`, `servers_private`, and `clients` in that file.

```bash
python3 tools/run_experiment.py ycsb --settings experiments/settings-scale.json --out-dir ~/data/detock/
```

## Comparison to CockroachDB

This experiment is deployed in the following regions:
- us-east-1 
- us-east-2
- us-west-1
- us-west-2
- eu-west-1 
- eu-west-2

Use the "Full deployment" instructions above to deploy the system in these regions.

The following commands use a different settings file (`cockroach/settings.json`), so make sure to update the `servers_public`, `servers_private`, and `clients` in that file.

```bash
python3 tools/run_experiment.py ycsb --settings experiments/cockroach/settings.json --out-dir ~/data/detock/
```

To run the experiment in CockroachDB, set up a Kubernetes cluster in the same regions and use the configurations in [this repository](https://github.com/ctring/cockroach-aws/tree/main).

# Generate the results

Follow the instruction in [this repository](https://github.com/umd-dslam/DetockAnalysis) to generate the results.