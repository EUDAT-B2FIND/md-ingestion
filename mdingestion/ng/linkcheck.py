import socket
import queue
import threading
from os import path
import requests
from requests.exceptions import HTTPError
from requests.utils import requote_uri


class LinkChecker(object):
    """
    Checks for broken external links.

    Taken from Sphinx:
    https://www.sphinx-doc.org/en/2.0/_modules/sphinx/builders/linkcheck.html
    """
    def __init__(self):
        self.to_ignore = []
        self.good = set()
        self.broken = {}
        self.redirected = {}
        # set a timeout for non-responding servers
        socket.setdefaulttimeout(5.0)
        # create output file
        # open(path.join(self.outdir, 'output.txt'), 'w').close()
        # create queues and worker threads
        self.wqueue = queue.Queue()
        # self.rqueue = queue.Queue()
        self.workers = []
        for i in range(5):
            thread = threading.Thread(target=self.check_thread)
            thread.setDaemon(True)
            thread.start()
            self.workers.append(thread)

    def check_thread(self):
        kwargs = {
            'allow_redirects': True,
            'headers': {
                'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',  # noqa
            },
        }
        # kwargs['timeout'] = None

        def check_uri():
            req_url = uri

            # handle non-ASCII URIs
            try:
                req_url.encode('ascii')
            except UnicodeError:
                req_url = requote_uri(req_url)

            try:
                try:
                    # try a HEAD request first, which should be easier on
                    # the server and the network
                    response = requests.head(req_url, **kwargs)
                    response.raise_for_status()
                except HTTPError:
                    # retry with GET request if that fails, some servers
                    # don't like HEAD requests.
                    response = requests.get(req_url, stream=True, **kwargs)
                    response.raise_for_status()
            except HTTPError as err:
                if err.response.status_code == 401:
                    # We'll take "Unauthorized" as working.
                    return 'working', ' - unauthorized', 0
                elif err.response.status_code == 503:
                    # We'll take "Service Unavailable" as ignored.
                    return 'ignored', str(err), 0
                else:
                    return 'broken', str(err), 0
            except Exception as err:
                return 'broken', str(err), 0
            if response.url.rstrip('/') == req_url.rstrip('/'):
                return 'working', '', 0
            else:
                new_url = response.url
                # history contains any redirects, get last
                if response.history:
                    code = response.history[-1].status_code
                    return 'redirected', new_url, code
                else:
                    return 'redirected', new_url, 0

        def check():
            # check for various conditions without bothering the network
            if len(uri) == 0 or uri.startswith(('#', 'mailto:', 'ftp:')):
                return 'unchecked', '', 0
            elif not uri.startswith(('http:', 'https:')):
                return 'local', '', 0
            elif uri in self.good:
                return 'working', 'old', 0
            elif uri in self.broken:
                return 'broken', self.broken[uri], 0
            elif uri in self.redirected:
                return 'redirected', self.redirected[uri][0], self.redirected[uri][1]
            for rex in self.to_ignore:
                if rex.match(uri):
                    return 'ignored', '', 0

            # need to actually check the URI
            for _ in range(1):
                status, info, code = check_uri()
                if status != "broken":
                    break

            if status == "working":
                self.good.add(uri)
            elif status == "broken":
                self.broken[uri] = info
            elif status == "redirected":
                self.redirected[uri] = (info, code)

            return status

        while True:
            uri = self.wqueue.get()
            if uri is None:
                break
            check()
            # self.rqueue.put((uri, status))

    def add(self, doc):
        self.wqueue.put(doc.doi, False)
        self.wqueue.put(doc.pid, False)
        self.wqueue.put(doc.source, False)
        for url in doc.related_identifier:
            self.wqueue.put(url, False)
        self.wqueue.put(doc.metadata_access, False)
