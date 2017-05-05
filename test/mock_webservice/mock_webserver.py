#!/usr/bin/env python
import io
import http.server
import http.server
import socketserver
import sys

PORT=8888

#
# subclass of SimpleHTTPServer that always says "Ok."
#
class MyHandler(http.server.SimpleHTTPRequestHandler):
     def send_head(self):
          if self.path in [ '/poms/active_jobs', ]:
              resp = '[]\n'
              content_type = 'application/json'
          else:
              resp = "Ok.\n"
              content_type = 'text/plain'

          fresp = io.StringIO(resp)
          self.send_response(200)
          self.send_header("Content-type", content_type)
          self.send_header("Content-length", str(len(resp)))
          self.end_headers()
          return fresp

     def do_POST(self):
          content_length = int(self.headers['Content-Length'])
          pd = self.rfile.read(content_length)
          sys.stderr.write('post_data = {"%s"}\n' % pd.replace('=','": "').replace('&','","'))
          sys.stderr.flush()
          return self.do_GET()
         
def run_while_true(server_class = http.server.HTTPServer,
                    handler_class = MyHandler):
    keep_running = True
    server_address = ('127.0.0.1',PORT)
    httpd = server_class(server_address, handler_class)
    while keep_running:
         try:
            httpd.handle_request()
         except KeyboardInterrupt:
            #print "bailing..."
            keep_running = False

print("serving at port", PORT)

run_while_true()
