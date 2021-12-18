import argparse
import itertools
import json
import os
import logging
from tempfile import gettempdir
from multiprocessing import Process

import google.protobuf.text_format as text_format

import admin
from proto.configuration_pb2 import Configuration, Replica

LOG = logging.getLogger("experiment")

GENERATORS = 2


def generate_config(settings: dict, template_path: str):
    config = Configuration()
    with open(template_path, "r") as f:
        text_format.Parse(f.read(), config)

    regions_ids = {name: id for id, name in enumerate(settings["regions"])}
    for r in settings["regions"]:
        replica = Replica()

        servers_private = [addr.encode() for addr in settings["servers_private"][r]]
        replica.addresses.extend(servers_private)

        servers_public = [addr.encode() for addr in settings["servers_public"][r]]
        replica.public_addresses.extend(servers_public)

        clients = [addr.encode() for addr in settings["clients"][r]]
        replica.client_addresses.extend(clients)

        distance_ranking = [
            str(regions_ids[other_r]) for other_r in settings["distance_ranking"][r]
        ]
        replica.distance_ranking = ",".join(distance_ranking)

        config.replicas.append(replica)
        config.num_partitions = len(replica.addresses)

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
            "--clients", "0",
            "--txns", "0",
        ]
    )
    # fmt: on


def apply_filters(workload_settings: dict, val: dict):
    if "filters" not in workload_settings:
        return val

    for filter in workload_settings["filters"]:
        v, changed = apply_filter(filter, val)
        if changed:
            return v

    return val


def apply_filter(filter, val):
    matched = False
    for cond in filter["match"]:
        if eval_cond_and(cond, val):
            matched = True
            break
    if matched:
        action = filter["action"]
        if action == "change":
            v = action_change(filter["args"], val)
            return v, True
        elif action == "remove":
            return None, True
        else:
            raise Exception(f"Invalid action: {action}")

    return val, False


def eval_cond_and(cond, val):
    for op in cond:
        if not eval_op(op, cond[op], val):
            return False
    return True


def eval_cond_or(cond, val):
    for op in cond:
        if eval_op(op, cond[op], val):
            return True
    return False


def eval_op(op, op_val, val):
    if op == "or":
        return eval_cond_or(op_val, val)
    if op == "and":
        return eval_cond_and(op_val, val)
    not_in = op.endswith("~")
    key = op[:-1] if not_in else op
    return (not_in and val[key] not in op_val) or (not not_in and val[key] in op_val)


def action_change(args, val):
    new_val = dict.copy(val)
    for k, v in args.items():
        new_val[k] = v
    return new_val


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
        "servers_public": { string: [string] },   // Public IP addresses of all servers in each region
        "servers_private": { string: [string] },  // Private IP addresses of all servers in each region
        "clients": { string: [string] },          // Private IP addresses of all clients in each region

        // The objects from this point correspond to the experiments. Each object contains parameters
        // to run for an experiment. The experiment is run for all cross-combinations of all these parameters.
        <experiment name>: {
            "servers": [ { "config": string, "image": string } ], // A list of objects containing path to a config file and the Docker image used
            "workload": string,                                   // Name of the workload to use in this experiment
            <parameters>: [<parameter value>]                     // Parameters of the experiment
            "filters": [{                                         // A list of "if 'match' then do 'action'" over the parameter combinations.
                                                                  // The evaluation stops at the first match
                "match": [{<parameter>}],                         // A list of conditions that AND together. Each condition is an object listing
                                                                  // the values that need to match for some parameter. If a parameter name ends
                                                                  // with "~" then it is a NOT condition. An "or" object can be used for OR-ing the
                                                                  // conditions.
                "action": <"change" or "remove">                  // Action to perform on match
                "args": {}                                        // Arguments for the "change" action
            }],
        }
    }

    Example filters:
        "filters": [
            {
                // Remove every combination where hot is not 10000 or mh is not 50
                "match": [{"or": {"hot~": [10000], "mh~": [50]}}],
                "action": "remove"
            },
            {
                // Changes duration to 20 for all combinations with clients equals to 200
                "match": [{"clients": [200]}],
                "action": "change",
                "args": {
                    "duration": 20
                }
            }
        ]
    """

    NAME = ""
    # ARGS are the arguments of the benchmark tool other than the 'params' argument
    VARYING_ARGS = ["clients", "txns", "duration", "startup_spacing"]
    # PARAMS are the parameters of a workload specified in the 'params' argument of the benchmark tool
    VARYING_PARAMS = []

    @classmethod
    def run(cls, args):
        with open(os.path.join(args.config_dir, "settings.json"), "r") as f:
            settings = json.load(f)

        sample = settings.get("sample", 10)
        trials = settings.get("trials", 1)
        workload_setting = settings[cls.NAME]
        out_dir = os.path.join(
            args.out_dir, cls.NAME if args.name is None else args.name
        )

        for server in workload_setting["servers"]:
            config_path = generate_config(
                settings, os.path.join(args.config_dir, server["config"])
            )

            LOG.info('============ GENERATED CONFIG "%s" ============', config_path)

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

            # Compute the Cartesian product of all varying values
            varying_keys = cls.VARYING_ARGS + cls.VARYING_PARAMS
            ordered_value_lists = []
            for k in varying_keys:
                if k not in workload_setting:
                    raise KeyError(f"Missing required key in workload setting: {k}")
                ordered_value_lists.append(workload_setting[k])

            varying_values = itertools.product(*ordered_value_lists)
            values = [dict(zip(varying_keys, v)) for v in varying_values]

            if args.tag_keys:
                tag_keys = args.tag_keys
            else:
                tag_keys = [k for k in varying_keys if len(workload_setting[k]) > 1]

            for val in values:
                v = apply_filters(settings[cls.NAME], val)
                if v is None:
                    print(f"SKIP {val}")
                    continue
                for t in range(trials):
                    tag = config_name
                    tag_suffix = "".join([f"{k}{v[k]}" for k in tag_keys])
                    if tag_suffix:
                        tag += "-" + tag_suffix
                    if trials > 1:
                        tag += f"-{t}"

                    params = ",".join(f"{k}={v[k]}" for k in cls.VARYING_PARAMS)

                    LOG.info("RUN BENCHMARK")
                    # fmt: off
                    benchmark_args = [
                        "benchmark",
                        *common_args,
                        "--workload", workload_setting["workload"],
                        "--clients", f"{v['clients']}",
                        "--generators", f"{GENERATORS}",
                        "--txns", f"{v['txns']}",
                        "--duration", f"{v['duration']}",
                        "--startup-spacing", f"{v['startup_spacing']}",
                        "--sample", f"{sample}",
                        "--seed", "0",
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


class YCSBExperiment(Experiment):
    NAME = "ycsb"
    VARYING_PARAMS = [
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


class YCSBLatencyExperiment(Experiment):
    NAME = "ycsb-latency"
    VARYING_PARAMS = [
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


class YCSBNetworkExperiment(Experiment):
    NAME = "ycsb-network"
    VARYING_PARAMS = [
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


class TPCCExperiment(Experiment):
    NAME = "tpcc"
    VARYING_PARAMS = ["mh_zipf", "sh_only"]


class CockroachExperiment(Experiment):
    NAME = "cockroach"
    VARYING_PARAMS = ["records", "hot", "mh"]


class CockroachLatencyExperiment(Experiment):
    NAME = "cockroach-latency"
    VARYING_PARAMS = ["records", "hot", "mh"]


if __name__ == "__main__":

    EXPERIMENTS = {
        "ycsb": YCSBExperiment(),
        "ycsb-latency": YCSBLatencyExperiment(),
        "ycsb-network": YCSBNetworkExperiment(),
        "tpcc": TPCCExperiment(),
        "cockroach": CockroachExperiment(),
        "cockroach-latency": CockroachLatencyExperiment(),
    }

    parser = argparse.ArgumentParser(description="Run an experiment")
    parser.add_argument(
        "experiment", choices=EXPERIMENTS.keys(), help="Name of the experiment to run"
    )
    parser.add_argument(
        "--config-dir", "-c", default="config", help="Path to the configuration files"
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
        help="Keys to include in the tag. If empty, only include",
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
    args = parser.parse_args()

    if args.dry_run:

        def noop(cmd):
            print("\t" + " ".join(cmd))

        admin.main = noop

    EXPERIMENTS[args.experiment].run(args)
