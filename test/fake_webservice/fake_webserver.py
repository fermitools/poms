import StringIO
import SimpleHTTPServer
import SocketServer
import sys

PORT=8888

#
# subclass of SimpleHTTPServer that always says "Ok."
#
class MyHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
     def send_head(self):
          if self.path in [ '/active_jobs', ]:
              resp = '[]\n'
              content_type = 'application/json'
          else:
              resp = "Ok.\n"
              content_type = 'text/plain'

          fresp = StringIO.StringIO(resp)
          self.send_response(200)
          self.send_header("Content-type", content_type)
          self.send_header("Content-length", str(len(resp)))
          self.end_headers()
          return fresp

     def do_POST(self):
          content_length = int(self.headers['Content-Length'])
          pd = self.rfile.read(content_length)
          sys.stderr.write("\nPOST data: ------\n%s\n------\n" % pd)
          sys.stderr.flush()
          return self.do_GET()
         
httpd = SocketServer.TCPServer(("127.0.0.1",PORT), MyHandler)

print "serving at port", PORT
httpd.serve_forever()

