import base64
import http.client
import os
import socket
import socketserver
import threading
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlsplit

HOST = "127.0.0.1"
PORT = int(os.environ.get("PROXY_PORT", "8085"))
TOKEN = os.environ.get("PROXY_TOKEN", "nse-proxy-2026")

CONNECT_TIMEOUT = 15
IDLE_TIMEOUT = 120


def _authorized(headers):
    p = headers.get("Proxy-Authorization")
    if not p or not p.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(p[6:]).decode("utf-8", "ignore")
    except Exception:
        return False
    return decoded.split(":", 1)[0] == TOKEN


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _reject(self, code, message):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.flush()

    def _auth_ok(self):
        if not _authorized(self.headers):
            self.send_response(407)
            self.send_header("Proxy-Authenticate", 'Basic realm="proxy"')
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return False
        return True

    def do_CONNECT(self):
        if not self._auth_ok():
            return
        host, _, port = self.path.partition(":")
        try:
            port = int(port or 443)
        except ValueError:
            self._reject(400, "bad port")
            return
        try:
            upstream = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)
        except Exception:
            self._reject(502, "cannot reach target")
            return

        self.send_response(200, "Connection established")
        self.end_headers()
        self.wfile.flush()

        self.connection.settimeout(IDLE_TIMEOUT)
        upstream.settimeout(IDLE_TIMEOUT)
        done = threading.Event()

        def pump(src, dst):
            try:
                while not done.is_set():
                    try:
                        data = src.recv(65536)
                    except (BlockingIOError, socket.timeout):
                        continue
                    if not data:
                        break
                    dst.sendall(data)
            except Exception:
                pass
            finally:
                done.set()
                try:
                    upstream.close()
                except Exception:
                    pass
                try:
                    self.connection.close()
                except Exception:
                    pass

        t1 = threading.Thread(target=pump, args=(self.connection, upstream), daemon=True)
        t2 = threading.Thread(target=pump, args=(upstream, self.connection), daemon=True)
        t1.start()
        t2.start()
        t1.join()

    def _forward_http(self):
        if not self._auth_ok():
            return
        parts = urlsplit(self.path)
        if not parts.netloc:
            self._reject(400, "bad url")
            return
        target_port = parts.port or (443 if parts.scheme == "https" else 80)
        cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
        path = parts.path or "/"
        if parts.query:
            path += "?" + parts.query
        try:
            conn = cls(parts.hostname, target_port, timeout=CONNECT_TIMEOUT)
            conn.request(self.command, path, headers={"Host": parts.netloc, "Connection": "close"})
            resp = conn.getresponse()
            body = resp.read()
        except Exception:
            self._reject(502, "upstream error")
            return
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("content-length", "connection", "transfer-encoding"):
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass
        conn.close()

    do_GET = _forward_http
    do_POST = _forward_http
    do_PUT = _forward_http
    do_DELETE = _forward_http
    do_HEAD = _forward_http
    do_OPTIONS = _forward_http


class ProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    import base64 as b64mod

    server = ProxyServer((HOST, PORT), ProxyHandler)
    auth = "Basic " + b64mod.b64encode((TOKEN + ":x").encode()).decode()
    print("=" * 56)
    print("  Indian NSE proxy running on  %s:%s" % (HOST, PORT))
    print("  Token:                        %s" % TOKEN)
    print("  Render NSE_PROXY value:")
    print("    http://%s:anything@PUBLIC_IP:%s" % (TOKEN, PORT))
    print("=" * 56)
    print("  (Leave this window open. Close it to stop the proxy.)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nProxy stopped.")
