'''Prometheus exporter class implementation.'''

from datetime import datetime
from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import logging
from threading import Thread, Lock

class PrometheusExporter():
    '''Manage all measurements and provide the htp interface for interfacing with
    Prometheus.'''

    def __init__(self, interface, port):
        '''Initialize the exporter. Does not yet start the http server thread.

        :param interface: The network interface to bind the HTTP server. Use
          0.0.0.0 to bind to all interfaces.
        :param int port: The tcp port number to listen on for HTTP requests.'''

        self._interface = interface
        self._port = port

        # prom: dict(m_name: measurement)
        # measurement: dict('help':str, 'type':str, 'data': items, 'timeout':int)
        # items: dict(i_name: item)
        # item: dict('value': ?, 'timestamp': datetime)
        self._prom = {}
        self._lock = Lock()


    def register(self, m_name, datatype, helpstr, timeout=None):
        '''Register a name for exporting. This must be called before calling
        `set()`.

        :param str name: The name to register.
        :param str type: One of gauge or counter.
        :param str helpstr: The help information / comment to include in the
          output.
        :param int timeout: Timeout in seconds for any value. Before rendering,
          values which are updated longer ago than this value, are removed.'''

        with self._lock:
            if m_name in self._prom:
                raise Exception('Measurement already registered')

            measurement = {
                'help': helpstr,
                'type': datatype,
                'data': {},
                'timeout': timeout
            }

            self._prom[m_name] = measurement


    def set(self, m_name, labels, value, fmt='{0}'):
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

        if m_name not in self._prom:
            msg = "Cannot set unknown measurement '{0}'."
            logging.error(msg.format(m_name))
            return

        i_name = '{m_name}{{{labels}}}'.format(
            m_name=m_name,
            labels=labelstr)

        with self._lock:
            items = self._prom[m_name]['data']

            if value is not None:
                items[i_name] = {
                    'value': fmt.format(value),
                    'timestamp': datetime.now(),
                }
            else:
                # we remove the item when passing None as value
                if i_name in items:
                    del items[i_name]

        msg = 'Set prom value {0} = {1}'
        logging.debug(msg.format(i_name, value))


    def _check_timeout(self):
        '''Remove all data which has timed out (i.e. is not valid anymore).'''

        to_delete = []

        # loop over all measurements
        for measurement in self._prom.values():
            timeout = measurement['timeout']

            # check if timeout is 0 or None
            if not timeout:
                continue

            items = measurement['data']
            now = datetime.now()

            # first loop to find timed out items
            for i_name, item in items.items():
                if (now - item['timestamp']).total_seconds() >= timeout:
                    to_delete.append(i_name)

            # second loop to remove them
            for i_name in to_delete:
                del items[i_name]
                msg = "Removed timed out item '{0}'."
                logging.debug(msg.format(i_name))

            to_delete.clear()


    def render(self):
        '''Render the current data to Prometheus format. See
        https://prometheus.io/docs/instrumenting/exposition_formats/ for details.

        :returns: String with output suitable for consumption by Prometheus over
          HTTP. '''

        lines = []

        with self._lock:

            self._check_timeout()

            for m_name, measurement in self._prom.items():
                items = measurement['data']

                # do not output items without values (checks for empty list)
                if not items:
                    continue

                lines.append('# HELP {0} {1}'.format(
                    m_name,
                    measurement['help']))
                lines.append('# TYPE {0} {1}'.format(
                    m_name,
                    measurement['type']))

                for i_name, item in items.items():
                    lines.append('{0} {1}'.format(
                        i_name,
                        item['value']))

        return '\n'.join(lines)


    def _run_http_server(self):
        '''Start the http server to serve the prometheus data. This function
        does not return.'''

        msg = 'Starting http server on {interface}:{port}.'
        logging.info(msg.format(
            interface=self._interface,
            port=self._port))

        httpd = ThreadingHTTPServer(
            (self._interface, self._port),
            PromHttpRequestHandler)

        # we attach our own instance to the server object, so that the request
        # handler later can access it.
        httpd.prom_exp = self

        httpd.serve_forever()


    def start_server_thread(self):
        '''Create a thread to run the http server serving the prometheus data.'''

        srv_thread = Thread(
            target=self._run_http_server,
            name='http_server',
            daemon=True)
        srv_thread.start()


class PromHttpRequestHandler(BaseHTTPRequestHandler):
    '''HTTP handler do answer GET requests with the metrics data.'''

    #pylint: disable=invalid-name
    def do_GET(self):
        '''Handle GET requests to the /metrics URL. Requests to any other URL get
        an info message as response.'''

        if self.path == '/metrics':
            prom_exp = self.server.prom_exp

            self.send_response(HTTPStatus.OK)
            self.end_headers()
            self.wfile.write(prom_exp.render().encode('utf-8'))
        else:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()
            self.wfile.write(b'URL not available. Please use /metrics path.')
