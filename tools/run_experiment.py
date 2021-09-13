import argparse
import itertools
import json
import os
import logging
import time
from tempfile import gettempdir
from multiprocessing import Process

import google.protobuf.text_format as text_format

import admin
from proto.configuration_pb2 import Configuration, Replica

LOG = logging.getLogger("experiment")

GENERATORS = 2

class Experiment:

    def __init__(self):
        self.settings = {}

    def generate_config(self, template_path: str):
        config = Configuration()
        with open(template_path, "r") as f:
            text_format.Parse(f.read(), config)

        regions_ids = {
            name : id for id, name in enumerate(self.settings['regions'])
        }
        for r in self.settings['regions']:
            replica = Replica()
            servers_private = [addr.encode() for addr in self.settings['servers_private'][r]]
            replica.addresses.extend(servers_private)
            servers_public = [addr.encode() for addr in self.settings['servers_public'][r]]
            replica.public_addresses.extend(servers_public)
            clients = [addr.encode() for addr in self.settings['clients'][r]]
            replica.client_addresses.extend(clients)
            distance_ranking = [str(regions_ids[other_r]) for other_r in self.settings['distance_ranking'][r]]
            replica.distance_ranking = ','.join(distance_ranking)
            config.replicas.append(replica)
            config.num_partitions = len(replica.addresses)
        
        config_path = os.path.join(gettempdir(), os.path.basename(template_path))
        with open(config_path, "w") as f:
            text_format.PrintMessage(config, f)
        
        return config_path


    def cleanup(self, config, image):
        admin.main([
            "benchmark",
            config,
            "--user", self.settings['username'],
            "--image", image,
            "--cleanup",
            "--clients", "0",
            "--txns", "0"
        ])


    def collect_client_data(self, config, out_dir, tag):
        admin.main([
            "collect_client",
            config,
            tag,
            "--user", self.settings['username'],
            "--out-dir", out_dir
        ])


    def collect_server_data(self, config, image, out_dir, tag):
        admin.main([
            "collect_server",
            config,
            "--tag", tag,
            "--user", self.settings['username'],
            "--image", image,
            "--out-dir", out_dir,
            # The image has already been pulled when starting the servers
            "--no-pull",
        ])

    NAME = ""
    # ARGS are the arguments of the benchmark tool other than the 'params' argument
    VARYING_ARGS = []
    # PARAMS are the parameters of a workload specified in the 'params' argument of the benchmark tool
    VARYING_PARAMS = []

    def run(self, args):
        with open(os.path.join(args.config_dir, "settings.json"), "r") as f:
            self.settings = json.load(f)

        sample = self.settings.get("sample", 10)
        trials = self.settings.get("trials", 1)
        workload_setting = self.settings[self.NAME]
        out_dir = os.path.join(args.out_dir, self.NAME if args.name is None else args.name)

        for server in workload_setting["servers"]:
            config = self.generate_config(os.path.join(args.config_dir, server['config']))

            LOG.info('============ GENERATED CONFIG "%s" ============', config)

            config_name = os.path.splitext(os.path.basename(server['config']))[0]
            image = server['image']
            common_args = [config, "--user", self.settings['username'], "--image", image]

            LOG.info("STOP ANY RUNNING EXPERIMENT")
            self.cleanup(config, image)

            if not args.skip_starting_server:
                LOG.info("START SERVERS")
                admin.main(["start", *common_args])
        
                LOG.info("WAIT FOR ALL SERVERS TO BE ONLINE")
                admin.main(["collect_server", *common_args, "--flush-only", "--no-pull"])

            # Compute the Cartesian product of all varying values
            varying_keys = self.VARYING_ARGS + self.VARYING_PARAMS
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
                v = self.__apply_filters(val)
                if v is None:
                    print(f"SKIP {val}")
                    continue
                for t in range(trials):
                    tag = config_name
                    tag_suffix = ''.join([f"{k}{v[k]}" for k in tag_keys])
                    if tag_suffix:
                        tag += "-" + tag_suffix
                    if trials > 1:
                        tag += f"-{t}"

                    params = ','.join(f"{k}={v[k]}" for k in self.VARYING_PARAMS)

                    LOG.info("RUN BENCHMARK")
                    admin.main([
                        "benchmark",
                        *common_args,
                        "--workload", workload_setting['workload'],
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
                        "--no-pull"
                    ])

                    LOG.info("COLLECT DATA")
                    collectors = []
                    if not args.no_client_data:
                        collectors.append(Process(target=self.collect_client_data, args=(config, out_dir, tag)))
                    if not args.no_server_data:
                        collectors.append(Process(target=self.collect_server_data, args=(config, image, out_dir, tag)))
                    for p in collectors:
                        p.start()
                    for p in collectors:
                        p.join()

    def __apply_filters(self, val):
        workload_settings = self.settings[self.NAME]
        if "filters" not in workload_settings:
            return val

        for filter in workload_settings["filters"]:
            v, changed = self.__filter(filter, val)
            if changed:
                return v

        return val

    def __filter(self, filter, val):
        matched = False
        for cond in filter["match"]:
            if self.__eval_cond_and(cond, val):
                matched = True
                break
        if matched:
            action = filter["action"]
            if action == "change":
                v = self.__action_change(filter["args"], val)
                return v, True
            elif action == "remove":
                return None, True
            else:
                raise Exception(f"Invalid action: {action}")
        
        return val, False

    def __eval_cond_and(self, cond, val):
        for op in cond:
            if not self.__eval_op(op, cond[op], val):
                return False
        return True

    def __eval_cond_or(self, cond, val):
        for op in cond:
            if self.__eval_op(op, cond[op], val):
                return True
        return False

    def __eval_op(self, op, op_val, val):
        if op == "or":
            return self.__eval_cond_or(op_val, val)
        if op == "and":
            return self.__eval_cond_and(op_val, val)
        not_in = op.endswith("~")
        key = op[:-1] if not_in else op
        return (not_in and val[key] not in op_val) or (not not_in and val[key] in op_val)

    def __action_change(self, args, val):
        new_val = dict.copy(val)
        for k, v in args.items():
            new_val[k] = v
        return new_val


class YCSBExperiment(Experiment):
    NAME = "ycsb"
    VARYING_ARGS = ["clients", "txns", "duration", "startup_spacing"]
    VARYING_PARAMS = ["writes", "records", "hot_records", "mp_parts", "mh_homes", "mh_zipf", "hot", "mp", "mh"]


class YCSBLatencyExperiment(Experiment):
    NAME = "ycsb-latency"
    VARYING_ARGS = ["clients", "txns", "duration", "startup_spacing"]
    VARYING_PARAMS = ["writes", "records", "hot_records", "mp_parts", "mh_homes", "mh_zipf", "hot", "mp", "mh"]


class TPCCExperiment(Experiment):
    NAME = "tpcc"
    VARYING_ARGS = ["clients", "txns", "duration"]
    VARYING_PARAMS = ["mh_zipf", "sh_only"]


if __name__ == "__main__":

    EXPERIMENTS = {
        "ycsb": YCSBExperiment(),
        "ycsb-latency": YCSBLatencyExperiment(),
        "tpcc": TPCCExperiment()
    }

    parser = argparse.ArgumentParser(description="Run an experiment")
    parser.add_argument("experiment", choices=EXPERIMENTS.keys(),
                        help="Name of the experiment to run")
    parser.add_argument("--config-dir", "-c", default="config", help="Path to the configuration files")
    parser.add_argument("--out-dir", "-o", default=".", help="Path to the output directory")
    parser.add_argument("--name", "-n", help="Override name of the experiment directory")
    parser.add_argument("--tag-keys", nargs="*", help="Keys to include in the tag. If empty, only include")
    parser.add_argument(
        "--dry-run",
        action='store_true',
        help="Check the settings and generate configs without running the experiment"
    )
    parser.add_argument("--skip-starting-server", action="store_true", help="Skip starting server step")
    parser.add_argument("--no-client-data", action="store_true", help="Don't collect client data")
    parser.add_argument("--no-server-data", action="store_true", help="Don't collect server data")
    args = parser.parse_args()

    if args.dry_run:
        def noop(cmd):
            print('\t' + ' '.join(cmd))
        admin.main = noop

    EXPERIMENTS[args.experiment].run(args)
