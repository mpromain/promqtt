'''Implementation of the HTTP server to provide the data to Prometheus.'''

import logging
from threading import Thread

from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class PromHttpRequestHandler(BaseHTTPRequestHandler):
    '''Implementation of the http request handler for a very basic webserver
    mainly serving the prometheus data and a few diagnostic pages.'''

    #pylint: disable=invalid-name
    def do_GET(self):
        '''Handler for HTTP GET method.

        This checks if the requested path is contained in the routes structure
        and then calls the specified rendering function. Otherwise it returns
        a 404 page.'''

        if self.path in self.server.srv.routes:
            route = self.server.srv.routes[self.path]

            self.send_response(200)
            self.send_header('Content-type', route['type'])
            self.end_headers()

            self.wfile.write(route['fct']().encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'URL not found. Please use /metrics path.')


class HttpServer():
    '''HTTP server thread implementation.'''

    def __init__(self, http_cfg, routes):
        self._http_cfg = http_cfg
        self._routes = routes


    @property
    def routes(self):
        '''Provides the routes dictionary for access by the HTTP request
        handler.

        :returns: The routes structure'''

        return self._routes


    def _run_http_server(self):
        '''Start the http server to serve the prometheus data. This function
        does not return.'''

        httpd = ThreadingHTTPServer(
            (self._http_cfg['interface'], self._http_cfg['port']),
            PromHttpRequestHandler)

        # we attach our own instance to the server object, so that the request
        # handler later can access it.
        httpd.srv = self

        httpd.serve_forever()


    def start_server_thread(self):
        '''Create a thread to run the http server serving the prometheus data.'''

        msg = 'Starting http server on {interface}:{port}.'
        logging.info(msg.format(**self._http_cfg))

        srv_thread = Thread(
            target=self._run_http_server,
            name='http_server',
            daemon=True)
        srv_thread.start()
