'''Application main module for setting up everything.'''

import argparse
import json
import logging
import os
import signal

from ruamel.yaml import YAML

from promqtt.__version__ import __title__, __version__
from promqtt.cfgdesc import CFG_DESC
from promqtt.configer import prepare_argparser, eval_cfg
from promqtt.http import HttpServer
from promqtt.prom import PrometheusExporter
from promqtt.tasmota import TasmotaMQTTClient


def sigterm_handler(signum, stack_frame):
    '''Handle the SIGTERM signal by shutting down.'''

    # Deal with unused variables according to pylint suggestion
    del signum
    del stack_frame

    logging.info('Terminating promqtt. Bye!')

    # TODO: Check if this can be done better. Need to terminate all threads.
    #pylint: disable=protected-access
    os._exit(0)


def parse_args():
    '''Set up the command-line parser and parse arguments.'''

    parser = argparse.ArgumentParser(
        description="Tasmota MQTT to Prometheus exporter.")

    prepare_argparser(CFG_DESC, parser)

    return parser.parse_args()


def export_build_info(promexp, version):
    '''Export build information for prometheus.'''

    promexp.register(
        name='tasmota_build_info',
        datatype='gauge',
        helpstr='Version info',
        timeout=None)

    promexp.set(
        name='tasmota_build_info',
        value='1',
        labels={'version': version})


def setup_logging(verbose):
    '''Configure the logging.'''

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='[%(levelname)s] (%(threadName)s) %(message)s')

    logging.info('Starting {0} {1}'.format(
        __title__,
        __version__))


def main():
    '''Main function of the promqtt tool.'''

    signal.signal(signal.SIGTERM, sigterm_handler)

    args = parse_args()

    # currently we do not load a config file
    filecfg = {}
    cfg = eval_cfg(CFG_DESC, filecfg, os.environ, args)
    setup_logging(cfg['verbose'])


    # load device configuration
    yaml = YAML(typ='safe')
    with open(cfg['cfgfile']) as filehdl:
        devcfg = yaml.load(filehdl)

    promexp = PrometheusExporter()
    export_build_info(promexp, __version__)

    routes = {
        '/metrics': {
            'type': 'text/plain',
            'fct': promexp.render
        },
        '/cfg_json': {
            'type': 'application/json',
            'fct': lambda: json.dumps(cfg, indent=4)
        },
        '/devcfg_json': {
            'type': 'application/json',
            'fct': lambda: json.dumps(devcfg, indent=4)
        },
    }

    httpsrv = HttpServer(http_cfg=cfg['http'], routes=routes)
    httpsrv.start_server_thread()

    tmc = TasmotaMQTTClient(promexp, mqtt_cfg=cfg['mqtt'], cfg=devcfg)
    tmc.loop_forever()


if __name__ == '__main__':
    main()
