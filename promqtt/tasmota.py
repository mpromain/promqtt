'''MQTT client implementation.'''

import json
import logging

import paho.mqtt.client as mqtt
from promqtt.device_loader import prepare_devices


def _is_topic_matching(ch_topic, msg_topic):
    '''Check if the msg_topic (already split as list) matches the ch_topic (also
    split as list).

    :param ch_topic: The pattern topic, already split at '/' as a list.
    :param msg_topic: The matching topic, split at '/' as a list.

    :return: True if msg_topic matches the pattern ch_topic.'''

    if len(ch_topic) != len(msg_topic):
        return False

    # check if all topic elements either match or equal the wildcard
    # character '+'
    result = all(
        (part in ('+', msg_topic[i]))
        for i, part in enumerate(ch_topic))

    return result


class TasmotaMQTTClient():
    '''MQTT client implementation'''

    def __init__(self, prom_exp, mqtt_cfg, cfg):
        self._prom_exp = prom_exp

        self._cfg = cfg

        prepare_devices(cfg)

        self._register_measurements()

        msg = 'Connecting to MQTT broker at {broker}:{port}.'
        logging.info(msg.format(**mqtt_cfg))
        self._mqttc = mqtt.Client()

        # register callback for received messages
        self._mqttc.on_message = self.on_mqtt_msg

        self._mqttc.connect(
            host=mqtt_cfg['broker'],
            port=mqtt_cfg['port'])

        sub_topic = '#'
        self._mqttc.subscribe(sub_topic)

        msg = "Tasmota client subscribing to '{0}'."
        logging.debug(msg.format(sub_topic))


    def loop_forever(self):
        '''Start the MQTT receiver loop. This function does not return.'''
        self._mqttc.loop_forever()


    def _register_measurements(self):
        '''Register measurements for prometheus.'''

        for name, meas in self._cfg['measurements'].items():
            msg = 'Registering measurement {0}'
            logging.debug(msg.format(name))
            self._prom_exp.register(
                name=name,
                datatype=meas['type'],
                helpstr=meas['help'],
                timeout=meas['timeout'] if meas['timeout'] else None)


    def on_mqtt_msg(self, client, obj, msg):
        '''Handle incoming MQTT message.'''

        # handle unused argument according to pylint suggestion
        del client
        del obj

        try:
            msg_data = {
                'raw_topic': msg.topic,
                'raw_payload': msg.payload,
                'topic': msg.topic.split('/'),
            }

            for dev in self._cfg['devices'].values():
                self._handle_device(dev, msg_data)

        #pylint: disable=broad-except
        except Exception:
            logging.exception('Failed to process a received MQTT message.')


    def _handle_device(self, dev, msg_data):
        for chnl in dev['channels'].values():
            if _is_topic_matching(chnl['topic'], msg_data['topic']):
                try:
                    self._handle_channel(dev, chnl, msg_data)
                #pylint: disable=broad-except
                except Exception:
                    msg = "Failed to handle device '{dev}', channel '{chnl}'."
                    logging.exception(msg.format(
                        dev=dev['_dev_name'],
                        chnl=chnl['_ch_name']))


    def _handle_channel(self, dev, chnl, msg_data):
        # Step 1: parse value
        if chnl['parse'] == 'json':
            value = json.loads(msg_data['raw_payload'])
        else:
            value = msg_data['raw_payload']

        # Step 2: Extract value from payload (e.g. a specific value from JSON
        # structure) by string formatting
        try:
            value = chnl['value'].format(dev=dev, chnl=chnl, msg=msg_data, value=value)
        except KeyError:
            msg = (
                "Failed to process value access in device '{dev}', "
                "channel '{chnl}', expression '{expr}' for payload '{payload}'."
            )
            logging.debug(msg.format(
                dev=dev['_dev_name'],
                chnl=chnl['_ch_name'],
                expr=chnl['value'],
                payload=msg_data['raw_payload']))
            return

        # Step 3: map string values to numeric values
        if 'map' in chnl:
            if value in chnl['map']:
                value = chnl['map'][value]
            else:
                value = float('nan')

        # Step 4: scale
        if ('factor' in chnl) or ('offset' in chnl):
            try:
                value = (
                    float(value)
                    * chnl.get('factor', 1.0)
                    + chnl.get('offset', 0.0))
            except ValueError:
                # generate "not a number" value
                value = float('nan')

        # legacy
        msg_data['val'] = value

        bind_labels = {
            lname.format(dev=dev, chnl=chnl, msg=msg_data):
            lval.format(dev=dev, chnl=chnl, msg=msg_data)
            for lname, lval in chnl['labels'].items()
        }

        measurement = chnl['measurement'].format(
            dev=dev,
            chnl=chnl,
            msg=msg_data,
            value=value)

        self._prom_exp.set(
            name=measurement,
            value=value,
            labels=bind_labels)
