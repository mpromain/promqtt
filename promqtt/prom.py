'''Rendering the data for Prometheus.'''

from datetime import datetime
import logging
from threading import Thread, Lock


class PrometheusExporterException(Exception):
    '''Base class for all exceptions generated by the PrometheusExporter.'''

    pass


class PrometheusExporter():
    '''Manage all measurements and provide the htp interface for interfacing with
    Prometheus.'''

    def __init__(self):
        self._prom = {}
        self._lock = Lock()


    def register(self, name, datatype, helpstr, timeout=None):
        '''Register a name for exporting. This must be called before calling
        `set()`.

        :param str name: The name to register.
        :param str type: One of gauge or counter.
        :param str helpstr: The help information / comment to include in the
          output.
        :param int timeout: Timeout in seconds for any value. Before rendering,
          values which are updated longer ago than this value, are removed.'''

        with self._lock:
            if name not in self._prom:
                self._prom[name] = {
                    'help': helpstr,
                    'type': datatype,
                    'data':{},
                    'timeout': timeout}
            else:
                raise PrometheusExporterException(
                    'Measurement already registered')


    def set(self, name, labels, value, fmt='{0}'):
        '''Set a value for exporting.

        :param str name: The name of the value to set. This name must have been
          registered already by calling `register()`.
        :param dict labels: The labels to attach to this name.
        :param value: The value to set. Automatically converted to string.
        :param fmt: The string format to use to convert value to a string.
          Default: '{0}'. '''

        labelstr = ','.join(
            ['{0}="{1}"'.format(k, labels[k]) for k in sorted(labels.keys())]
        )

        if name not in self._prom:
            msg = "Cannot set not registered measurement '{0}'."
            logging.error(msg.format(name))
            return

        namestr = '{name}{{{labels}}}'.format(
            name=name,
            labels=labelstr)

        with self._lock:
            data = self._prom[name]['data']

            if value is not None:
                data[namestr] = {'value': fmt.format(value)}
                data[namestr]['timestamp'] = self._get_time()
            else:
                # we remove the item when passing None as value
                if namestr in data:
                    del data[namestr]

        logging.debug('Set prom value {0} = {1}'.format(
            namestr, value))


    def _get_time(self):
        '''Return the current time as a datetime object.

        Wrapped in a function, so it can be stubbed for testing.'''

        return datetime.now()


    def _check_timeout(self):
        '''Remove all data which has timed out (i.e. is not valid anymore).'''
        to_delete = []

        # loop over all measurements
        for meas in self._prom.values():
            to = meas['timeout']

            if to == None:
                continue

            data = meas['data']
            now = self._get_time()

            # first loop to find timed out items
            for item_name, item in data.items():
                if (now - item['timestamp']).total_seconds() >= to:
                    to_delete.append(item_name)

            # second loop to remove them
            for item_name in to_delete:
                del data[item_name]
                msg = "Removed timed out item '{0}'."
                logging.debug(msg.format(item_name))

            to_delete.clear()


    def render(self):
        '''Render the current data to Prometheus format. See
        https://prometheus.io/docs/instrumenting/exposition_formats/ for details.

        :returns: String with output suitable for consumption by Prometheus over
          HTTP. '''

        lines = []

        with self._lock:

            self._check_timeout()

            for k in self._prom.keys():
                data = self._prom[k]['data']

                # do not output items without values
                if len(self._prom[k]['data']) == 0:
                       continue

                lines.append('# HELP {k} {h}'.format(
                    k=k,
                    h=self._prom[k]['help']))
                lines.append('# TYPE {k} {t}'.format(
                    k=k,
                    t=self._prom[k]['type']))

                for i in data.keys():
                    lines.append('{n} {v}'.format(
                        n=i, v=data[i]['value']))

        return '\n'.join(lines)
