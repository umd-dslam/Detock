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

GENERATORS = 5

def generate_config(template_path: str, settings: dict):
    config = Configuration()
    with open(template_path, "r") as f:
        text_format.Parse(f.read(), config)

    regions = settings['servers'].keys()
    for r in regions:
        replica = Replica()
        servers = [addr.encode() for addr in settings['servers'][r]]
        replica.addresses.extend(servers)
        clients = [addr.encode() for addr in settings['clients'][r]]
        replica.client_addresses.extend(clients)
        config.replicas.append(replica)
        config.num_partitions = len(replica.addresses)
    
    config_path = os.path.join(gettempdir(), os.path.basename(template_path))
    with open(config_path, "w") as f:
        text_format.PrintMessage(config, f)
    
    LOG.info('Generated config "%s"', config_path)
    return config_path


def cleanup(config, user, image):
    admin.main([
        "benchmark",
        config,
        "--user", user,
        "--image", image,
        "--cleanup",
        "--clients", "0",
        "--txns", "0"
    ])


def collect_client_data(config, user, out_dir, tag):
    admin.main([
        "collect_client",
        config,
        tag,
        "--user", user,
        "--out-dir", out_dir
    ])


def collect_server_data(config, user, image, out_dir, tag):
    admin.main([
        "collect_server",
        config,
        "--tag", tag,
        "--user", user,
        "--image", image,
        "--no-pull",
        "--out-dir", out_dir
    ])


def ycsb(args):
    settings = {}
    with open(os.path.join(args.config_dir, "settings.json"), "r") as f:
        settings = json.load(f)

    user = settings['username']

    varying_args = ["clients", "txns", "duration"]
    varying_params = [
        "writes", "records", "hot_records", "mp_parts", "mh_homes", "hot", "mp", "mh"
    ]

    workload_setting = settings["ycsb"]
    ycsb_out_dir = os.path.join(args.out_dir, "ycsb" if args.name is None else args.name)

    for server in workload_setting["servers"]:
        config = generate_config(os.path.join(args.config_dir, server['config']), settings)
        config_name = os.path.splitext(os.path.basename(server['config']))[0]
        image = server['image']
        common_args = [config, "--user", user, "--image", image]

        LOG.info("Cleaning up previous experiments...")
        cleanup(config, user, image)
 
        LOG.info("Starting servers...")
        admin.main(["start", *common_args])
  
        LOG.info("Waiting for all servers to be online...")
        admin.main(["collect_server", *common_args, "--flush-only", "--no-pull"])

        # Compute the Cartesian product of all varying values
        varying_keys = varying_args + varying_params
        ordered_value_lists = []
        for k in varying_keys:
            if k not in workload_setting:
                raise KeyError(f"Missing required key in workload setting: {k}")
            ordered_value_lists.append(workload_setting[k])

        varying_values = itertools.product(*ordered_value_lists)
        values = [dict(zip(varying_keys, v)) for v in varying_values]

        tag_keys = [k for k in varying_keys if len(workload_setting[k]) > 1]
        for v in values:
            tag = config_name + '-' + ''.join([f"{k}{v[k]}" for k in tag_keys])
            params = ','.join(f"{k}={v[k]}" for k in varying_params)

            LOG.info("Running benchmark...")
            admin.main([
                "benchmark",
                *common_args,
                "--clients", f"{v['clients']}",
                "--generators", f"{GENERATORS}",
                "--txns", f"{v['txns']}",
                "--duration", f"{v['duration']}",
                "--sample", "10",
                "--seed", "0",
                "--params", params,
                "--tag", tag,
                # The image has already been pulled in the cleanup step
                "--no-pull"
            ])

            time.sleep(2)

            LOG.info("Collecting data...")
            collectors = [
                Process(target=collect_client_data, args=(config, user, ycsb_out_dir, tag)),
                Process(target=collect_server_data, args=(config, user, image, ycsb_out_dir, tag)),
            ]
            for p in collectors:
                p.start()
            for p in collectors:
                p.join()


if __name__ == "__main__":

    EXPERIMENTS = {
        "ycsb": ycsb
    }

    parser = argparse.ArgumentParser(description="Run an experiment")
    parser.add_argument("experiment", choices=EXPERIMENTS.keys(),
                        help="Name of the experiment to run")
    parser.add_argument("--config-dir", "-c", default="config", help="Path to the configuration files")
    parser.add_argument("--out-dir", "-o", default=".", help="Path to the output directory")
    parser.add_argument("--name", "-n", help="Override name of the experiment directory")
    parser.add_argument(
        "--dry-run",
        action='store_true',
        help="Check the settings and generate configs without running the experiment"
    )
    args = parser.parse_args()

    if args.dry_run:
        def noop(args):
            print('\t' + ' '.join(args))
            print()
        admin.main = noop

    EXPERIMENTS[args.experiment](args)
