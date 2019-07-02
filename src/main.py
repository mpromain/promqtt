import argparse
import json
import logging
import os
import signal
import sys

import paho.mqtt.client as mqtt

from prom import PrometheusExporter
from tasmota_mqtt import TasmotaMQTTClient


def sigterm_handler(signum, stack_frame):
    logging.info('Terminating promqtt. Bye!')
    sys.exit(0)


# configuration with default values
_config = {
    'http_interface': '127.0.0.1',
    'http_port': 8000,
    'mqtt_broker': 'mqtt',
    'mqtt_port': 1883,
    'mqtt_prefix': 'tasmota',
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Tasmota MQTT to Prometheus exporter.")

    parser.add_argument(
        '-i', '--http-interface',
        help='Set the listening interface and port for the HTTP server, e.g. 127.0.0.1:8000',
        required=False)

    parser.add_argument(
        '-m', '--mqtt-broker',
        help='MQTT broker hostname and port, e.g mqtt:1883',
        required=False)


    parser.add_argument(
        '-p', '--mqtt-prefix',
        help='MQTT topic prefix',
        required=False)
    
    return parser.parse_args()
    

def eval_args(args):
    if args.http_interface is not None:
        iface, port = args.http_interface.split(':')
        _config['http_interface'] = iface
        _config['http_port'] = int(port)

    if args.mqtt_broker:
        host, port = args.mqtt_broker.split(':')
        _config['mqtt_broker'] = host
        _config['mqtt_port'] = int(port)

    if args.mqtt_prefix:
        _config['mqtt_prefix'] = args.mqtt_prefix


def main():
    args = parse_args()
    eval_args(args)

    logfmt = '[%(levelname)s] (%(threadName)s) %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logfmt)
    logging.info('Starting promqtt.')    

    signal.signal(signal.SIGTERM, sigterm_handler)

    pe = PrometheusExporter(
        http_iface=_config['http_interface'],
        http_port=_config['http_port'])
    pe.start_server_thread()

    msg = 'Connecting to MQTT broker at {0}:{1}.'
    logging.info(msg.format(_config['mqtt_broker'], _config['mqtt_port']))
    mqttc = mqtt.Client()
    mqttc.connect(
        host=_config['mqtt_broker'],
        port=_config['mqtt_port'])
    
    tmc = TasmotaMQTTClient(mqttc, pe, prefix=['tasmota'])

    logging.debug('Start to run mqtt loop.')
    mqttc.loop_forever()

    

if __name__ == '__main__':
    main()

