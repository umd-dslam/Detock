import argparse
import boto3
import copy
import csv
import itertools
import json
import os
import logging
import shlex
import random
from tempfile import gettempdir
from time import sleep
from multiprocessing import Process
from pprint import pprint

import google.protobuf.text_format as text_format
from numpy import isin

import admin
from proto.configuration_pb2 import Configuration, Region

LOG = logging.getLogger("experiment")


def generate_config(settings: dict, workload_settingss: dict, template_path: str):
    config = Configuration()
    with open(template_path, "r") as f:
        text_format.Parse(f.read(), config)

    regions_ids = {name: id for id, name in enumerate(settings["regions"])}
    for r in settings["regions"]:
        region = Region()

        servers_private = [addr.encode() for addr in settings["servers_private"][r]]
        region.addresses.extend(servers_private)

        servers_public = [addr.encode() for addr in settings["servers_public"][r]]
        region.public_addresses.extend(servers_public)

        clients = [addr.encode() for addr in settings["clients"][r]]
        region.client_addresses.extend(clients)

        distance_ranking = [
            str(other_r) if isinstance(other_r, int) else str(regions_ids[other_r])
            for other_r in settings["distance_ranking"][r]
        ]
        region.distance_ranking = ",".join(distance_ranking)

        if "num_replicas" in settings:
            region.num_replicas = settings["num_replicas"].get(r, 1)
        else:
            region.num_replicas = 1

        if "shrink_mh_orderer" in settings:
            region.shrink_mh_orderer = settings["shrink_mh_orderer"].get(r, False)

        region.sync_replication = settings.get("local_sync_replication", False)
        
        if "num_log_managers" in workload_settingss:
            config.num_log_managers = workload_settingss["num_log_managers"]            

        config.regions.append(region)

    config_path = os.path.join(gettempdir(), os.path.basename(template_path))
    with open(config_path, "w") as f:
        text_format.PrintMessage(config, f)

    return config_path


def cleanup(username: str, config_path: str, image: str):
    # fmt: off
    admin.main(
        [
            "benchmark",
            config_path,
            "--user", username,
            "--image", image,
            "--cleanup",
            "--clients", "1",
            "--txns", "0",
        ]
    )
    # fmt: on


def collect_client_data(username: str, config_path: str, out_dir: str, tag: str):
    admin.main(
        ["collect_client", config_path, tag, "--user", username, "--out-dir", out_dir]
    )


def collect_server_data(
    username: str, config_path: str, image: str, out_dir: str, tag: str
):
    # fmt: off
    admin.main(
        [
            "collect_server",
            config_path,
            "--tag", tag,
            "--user", username,
            "--image", image,
            "--out-dir", out_dir,
            # The image has already been pulled when starting the servers
            "--no-pull",
        ]
    )
    # fmt: on


def collect_data(
    username: str,
    config_path: str,
    image: str,
    out_dir: str,
    tag: str,
    no_client_data: bool,
    no_server_data: bool,
):
    collectors = []
    if not no_client_data:
        collectors.append(
            Process(
                target=collect_client_data, args=(username, config_path, out_dir, tag)
            )
        )
    if not no_server_data:
        collectors.append(
            Process(
                target=collect_server_data,
                args=(username, config_path, image, out_dir, tag),
            )
        )
    for p in collectors:
        p.start()
    for p in collectors:
        p.join()


def combine_parameters(params, default_params, workload_settings):
    common_values = {}
    ordered_value_lists = []
    for p in params:
        if p in workload_settings:
            value_list = workload_settings[p]
            if isinstance(value_list, list):
                ordered_value_lists.append([(p, v) for v in value_list])
            else:
                common_values[p] = value_list
  
    combinations = [
        dict(v) for v in
        itertools.product(*ordered_value_lists)
    ]

    # Apply combinations inclusion
    if "include" in workload_settings:
        patterns = workload_settings["include"]
        # Resize extra to be the same size as combinations
        extra = [{} for _ in range(len(combinations))]
        # List of extra combinations
        new = []
        for p in patterns:
            is_new = True
            for c, e in zip(combinations, extra):
                overlap_keys = p.keys() & c.keys()
                if all([c[k] == p[k] for k in overlap_keys]):
                    is_new = False
                    e.update(
                        {k:p[k] for k in p if k not in overlap_keys}
                    )
            if is_new:
                new.append(p)

        for c, e in zip(combinations, extra):
            c.update(e)
        combinations += new

    # Apply combinations exclusion
    if "exclude" in workload_settings:
        patterns = workload_settings["exclude"]
        combinations = [
            c for c in combinations if
            not any([c.items() >= p.items() for p in patterns])
        ]

    # Populate common values and check for missing/unknown params
    params_set = set(params)
    for c in combinations:
        c.update(common_values)
        for k, v in default_params.items():
            if k not in c:
                c[k] = v

        missing = params_set - c.keys()
        if missing:
            raise KeyError(f"Missing required param(s) {missing} in {c}")

        unknown = c.keys() - params_set
        if unknown:
            raise KeyError(f"Unknown param(s) {unknown} in {c}")
    
    return combinations
    

class Experiment:
    """
    A base class for an experiment.

    An experiment consists of a settings.json file and config files.

    A settings.json file has the following format:
    {
        "username": string,  // Username to ssh to the machines
        "sample": int,       // Sample rate, in percentage, of the measurements
        "regions": [string], // Regions involved in the experiment
        "distance_ranking": { string: [string] }, // Rank of distance to all other regions from closest to farthest for each region
        "num_replicas": { string: int },          // Number of replicas in each region
        "servers_public": { string: [string] },   // Public IP addresses of all servers in each region
        "servers_private": { string: [string] },  // Private IP addresses of all servers in each region
        "clients": { string: [string] },          // Private IP addresses of all clients in each region
        // The objects from this point correspond to the experiments. Each object contains parameters
        // to run for an experiment. The experiment is run for all cross-combinations of all these parameters.
        <experiment name>: {
            "servers": [ { "config": string, "image": string } ], // A list of objects containing path to a config file and the Docker image used
            "workload": string,                                   // Name of the workload to use in this experiment

            // Parameters of the experiment. All possible combinations of the parameters 
            // will be generated and possibly modified by "exclude" and "include". This is
            // similar to Github's matrix:
            //      https://docs.github.com/en/actions/using-jobs/using-a-matrix-for-your-jobs#excluding-matrix-configurations
            <parameter1>: [<parameter1 values>]
            <parameter2>: [<parameter2 values>]
            ...
            "exclude": [
                { <parameterX>: <parameterX value>, ... },
                ...
            ]
            "include": [
                { <parameterY>: <parameterY value>, ... },
                ...
            ]
        }
    }
    """

    NAME = ""
    # Parameters of the workload
    WORKLOAD_PARAMS = []
    # Parameters of the benchmark tool and the environment other than the 'params' argument of the workload
    OTHER_PARAMS = ["generators", "clients", "txns", "duration", "rate_limit"]
    DEFAULT_PARAMS = {
        "generators": 2,
        "rate_limit": 0,
        "txns": 2000000,
    }

    @classmethod
    def pre_run_hook(cls, _settings: dict, _dry_run: bool):
        pass

    @classmethod
    def post_config_gen_hook(cls, _settings: dict, _config_path: str, _dry_run: bool):
        pass

    @classmethod
    def pre_run_per_val_hook(cls, _val: dict, _dry_run: bool):
        pass

    @classmethod
    def run(cls, args):
        settings_dir = os.path.dirname(args.settings)
        with open(args.settings, "r") as f:
            settings = json.load(f)

        sample = settings.get("sample", 10)
        trials = settings.get("trials", 1)
        workload_settings = settings[cls.NAME]
        out_dir = os.path.join(
            args.out_dir, cls.NAME if args.name is None else args.name
        )

        cls.pre_run_hook(settings, args.dry_run)

        for server in workload_settings["servers"]:
            config_path = generate_config(
                settings, workload_settings, os.path.join(settings_dir, server["config"])
            )

            LOG.info('============ GENERATED CONFIG "%s" ============', config_path)

            cls.post_config_gen_hook(settings, config_path, args.dry_run)

            config_name = os.path.splitext(os.path.basename(server["config"]))[0]
            image = server["image"]
            # fmt: off
            common_args = [
                config_path,
                "--user", settings["username"],
                "--image", image,
            ]
            # fmt: on

            LOG.info("STOP ANY RUNNING EXPERIMENT")
            cleanup(settings["username"], config_path, image)

            if not args.skip_starting_server:
                LOG.info("START SERVERS")
                admin.main(["start", *common_args])

                LOG.info("WAIT FOR ALL SERVERS TO BE ONLINE")
                admin.main(
                    ["collect_server", *common_args, "--flush-only", "--no-pull"]
                )

            params = cls.OTHER_PARAMS + cls.WORKLOAD_PARAMS

            values = combine_parameters(params, cls.DEFAULT_PARAMS, workload_settings)

            if args.tag_keys:
                tag_keys = args.tag_keys
            else:
                # Only use keys that have varying values
                tag_keys = [
                    k for k in params if
                    any([v[k] != values[0][k] for v in values])
                ]

            for val in values:
                cls.pre_run_per_val_hook(val, args.dry_run)

                for t in range(trials):
                    tag = config_name
                    tag_suffix = "".join([f"{k}{val[k]}" for k in tag_keys])
                    if tag_suffix:
                        tag += "-" + tag_suffix
                    if trials > 1:
                        tag += f"-{t}"

                    params = ",".join(f"{k}={val[k]}" for k in cls.WORKLOAD_PARAMS)

                    LOG.info("RUN BENCHMARK")
                    # fmt: off
                    benchmark_args = [
                        "benchmark",
                        *common_args,
                        "--workload", workload_settings["workload"],
                        "--clients", f"{val['clients']}",
                        "--rate", f"{val['rate_limit']}",
                        "--generators", f"{val['generators']}",
                        "--txns", f"{val['txns']}",
                        "--duration", f"{val['duration']}",
                        "--sample", f"{sample}",
                        "--seed", f"{args.seed}",
                        "--params", params,
                        "--tag", tag,
                        # The image has already been pulled in the cleanup step
                        "--no-pull",
                    ]
                    # fmt: on
                    admin.main(benchmark_args)

                    LOG.info("COLLECT DATA")
                    collect_data(
                        settings["username"],
                        config_path,
                        image,
                        out_dir,
                        tag,
                        args.no_client_data,
                        args.no_server_data,
                    )

            if args.dry_run:
                pprint([{ k:v for k, v in p.items() if k in tag_keys} for p in values])


class YCSBExperiment(Experiment):
    NAME = "ycsb"
    WORKLOAD_PARAMS = [
        "writes",
        "records",
        "hot_records",
        "mp_parts",
        "mh_homes",
        "mh_zipf",
        "hot",
        "mp",
        "mh",
    ]
    DEFAULT_PARAMS = {**Experiment.DEFAULT_PARAMS, **{
        "writes": 10,
        "records": 10,
        "hot_records": 2,
        "mp_parts": 2,
        "mh_homes": 2,
        "mh_zipf": 1,
    }}


class YCSB2Experiment(YCSBExperiment):
    NAME = "ycsb2"


class YCSBLatencyExperiment(Experiment):
    NAME = "ycsb-latency"
    WORKLOAD_PARAMS = [
        "writes",
        "records",
        "hot_records",
        "mp_parts",
        "mh_homes",
        "mh_zipf",
        "hot",
        "mp",
        "mh",
    ]
    DEFAULT_PARAMS = {**Experiment.DEFAULT_PARAMS, **{
        "writes": 10,
        "records": 10,
        "hot_records": 2,
        "mp_parts": 2,
        "mh_homes": 2,
        "mh_zipf": 1,
    }}


class YCSBNetworkExperiment(Experiment):
    ec2_region = ""

    DELAY = [
        [0.1, 6, 33, 38, 74, 87, 106, 99],
        [6, 0.1, 38, 43, 66, 80, 99, 94],
        [33, 38, 0.1, 6, 101, 114, 92, 127],
        [38, 43, 6, 0.1, 105, 118, 86, 132],
        [74, 66, 101, 105, 0.1, 16, 36, 64],
        [87, 80, 114, 118, 16, 0.1, 36, 74],
        [106, 99, 92, 86, 36, 36, 0.1, 46],
        [99, 94, 127, 132, 64, 74, 46, 0.1],
    ]

    @classmethod
    def pre_run_hook(cls, _: dict, _dry_run: bool):
        cls.ec2_region = input("Enter AWS region: ")

    @classmethod
    def run_netem_script(cls, file_name):
        ssm_client = boto3.client("ssm", region_name=cls.ec2_region)
        res = ssm_client.send_command(
            Targets=[{"Key": "tag:role", "Values": ["server"]}],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [f"sudo /home/ubuntu/{file_name}.sh"]},
        )
        command_id = res["Command"]["CommandId"]

        sleep(1)

        invocations = ssm_client.list_command_invocations(CommandId=command_id)
        instances = [inv["InstanceId"] for inv in invocations["CommandInvocations"]]

        waiter = ssm_client.get_waiter("command_executed")
        for instance in instances:
            waiter.wait(
                CommandId=command_id,
                InstanceId=instance,
                PluginName="aws:RunShellScript",
            )
            print(f"Executed netem script {file_name}.sh for {instance}")


class YCSBAsymmetryExperiment(YCSBNetworkExperiment):
    NAME = "ycsb-asym"
    WORKLOAD_PARAMS = [
        "writes",
        "records",
        "hot_records",
        "mp_parts",
        "mh_homes",
        "mh_zipf",
        "hot",
        "mp",
        "mh",
    ]
    OTHER_PARAMS = Experiment.OTHER_PARAMS + ["asym_ratio"]
    DEFAULT_PARAMS = {**Experiment.DEFAULT_PARAMS, **{
        "writes": 10,
        "records": 10,
        "hot_records": 2,
        "mp_parts": 2,
        "mh_homes": 2,
        "mh_zipf": 1,
    }}

    FILE_NAME = "netem_asym_{}"

    @classmethod
    def post_config_gen_hook(cls, settings: dict, config_path: str, dry_run: bool):
        delay = copy.deepcopy(cls.DELAY)

        workload_settings = settings[cls.NAME]
        if "asym_ratio" not in workload_settings:
            raise KeyError(f"Missing required key: asym_ratio")

        if dry_run:
            return

        ratios = workload_settings["asym_ratio"]
        for r in ratios:
            for i in range(len(delay)):
                for j in range(i + 1, len(delay[i])):
                    total = delay[i][j] + delay[j][i]
                    delay[i][j] = total * r / 100
                    delay[j][i] = total * (100 - r) / 100
                    if random.randint(0, 1):
                        delay[i][j], delay[j][i] = delay[j][i], delay[i][j]

            file_name = cls.FILE_NAME.format(r)
            delay_path = os.path.join(gettempdir(), file_name + ".csv")
            with open(delay_path, "w") as f:
                writer = csv.writer(f)
                writer.writerows(delay)

            # fmt: off
            admin.main(
                [
                    "gen_netem",
                    config_path,
                    delay_path,
                    "--user", settings["username"],
                    "--out", file_name + ".sh",
                ]
            )
            # fmt: on

    @classmethod
    def pre_run_per_val_hook(cls, val: dict, dry_run: bool):
        ratio = val["asym_ratio"]
        if not dry_run:
            cls.run_netem_script(cls.FILE_NAME.format(ratio))
            sleep(5)


class YCSBJitterExperiment(YCSBNetworkExperiment):
    NAME = "ycsb-jitter"
    WORKLOAD_PARAMS = [
        "writes",
        "records",
        "hot_records",
        "mp_parts",
        "mh_homes",
        "mh_zipf",
        "hot",
        "mp",
        "mh",
    ]
    OTHER_PARAMS = Experiment.OTHER_PARAMS + ["jitter"]
    DEFAULT_PARAMS = {**Experiment.DEFAULT_PARAMS, **{
        "writes": 10,
        "records": 10,
        "hot_records": 2,
        "mp_parts": 2,
        "mh_homes": 2,
        "mh_zipf": 1,
    }}

    FILE_NAME = "netem_jitter"

    @classmethod
    def post_config_gen_hook(cls, settings: dict, config_path: str, dry_run: bool):
        workload_settings = settings[cls.NAME]
        if "jitter" not in workload_settings:
            raise KeyError(f"Missing required key: jitter")

        delay_path = os.path.join(gettempdir(), cls.FILE_NAME + ".csv")
        with open(delay_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(cls.DELAY)

        jitters = workload_settings["jitter"]
        for j in jitters:
            # fmt: off
            admin.main(
                [
                    "gen_netem",
                    config_path,
                    delay_path,
                    "--user", settings["username"],
                    "--out", f"{cls.FILE_NAME}_{j}.sh",
                    "--jitter", str(j/2),
                ]
            )
            # fmt: on

    @classmethod
    def pre_run_per_val_hook(cls, val: dict, dry_run: bool):
        jitter = val["jitter"]
        if not dry_run:
            cls.run_netem_script(f"{cls.FILE_NAME}_{jitter}")
            sleep(5)


class TPCCExperiment(Experiment):
    NAME = "tpcc"
    WORKLOAD_PARAMS = ["mh_zipf", "sh_only"]


class CockroachExperiment(Experiment):
    NAME = "cockroach"
    WORKLOAD_PARAMS = ["records", "hot", "mh"]


class CockroachLatencyExperiment(Experiment):
    NAME = "cockroach-latency"
    WORKLOAD_PARAMS = ["records", "hot", "mh"]


if __name__ == "__main__":

    EXPERIMENTS = {
        "ycsb": YCSBExperiment(),
        "ycsb2": YCSB2Experiment(),
        "ycsb-latency": YCSBLatencyExperiment(),
        "ycsb-asym": YCSBAsymmetryExperiment(),
        "ycsb-jitter": YCSBJitterExperiment(),
        "tpcc": TPCCExperiment(),
        "cockroach": CockroachExperiment(),
        "cockroach-latency": CockroachLatencyExperiment(),
    }

    parser = argparse.ArgumentParser(description="Run an experiment")
    parser.add_argument(
        "experiment", choices=EXPERIMENTS.keys(), help="Name of the experiment to run"
    )
    parser.add_argument(
        "--settings", "-s", default="experiments/settings.json", help="Path to the settings file"
    )
    parser.add_argument(
        "--out-dir", "-o", default=".", help="Path to the output directory"
    )
    parser.add_argument(
        "--name", "-n", help="Override name of the experiment directory"
    )
    parser.add_argument(
        "--tag-keys",
        nargs="*",
        help="Keys to include in the tag",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check the settings and generate configs without running the experiment",
    )
    parser.add_argument(
        "--skip-starting-server", action="store_true", help="Skip starting server step"
    )
    parser.add_argument(
        "--no-client-data", action="store_true", help="Don't collect client data"
    )
    parser.add_argument(
        "--no-server-data", action="store_true", help="Don't collect server data"
    )
    parser.add_argument("--seed", default=0, help="Seed for the random engine")
    args = parser.parse_args()

    if args.dry_run:

        def noop(cmd):
            print(f"\t{shlex.join(cmd)}\n")

        admin.main = noop

    EXPERIMENTS[args.experiment].run(args)
