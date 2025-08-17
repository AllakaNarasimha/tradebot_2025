from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import TCPServer
from urllib.parse import parse_qs
import threading
import ssl

class MyHandler(BaseHTTPRequestHandler):
    callback = None

    def do_GET(self):
        auth_code = None
        query_string = self.path.split('?', 1)[-1]
        print(f"Received GET request with query string: {query_string}")
        
        # Parse the query string
        query_params = parse_qs(query_string)
        
        # Print the extracted query parameters
        for key, values in query_params.items():
            print(f"{key}: {values[0]}")
            if key == "auth_code":
                if not auth_code:
                    auth_code = values[0]

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Request received and processed successfully')
        
        if auth_code:
            self.callback(auth_code)
            stop_server()

class StoppableHTTPServer(HTTPServer):
    def run(self):
        self.serve_forever()

class StoppableHTTPSServer(StoppableHTTPServer):
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.socket = ssl.wrap_socket(
            self.socket,
            certfile='C:\\Users\\allakn2\\AppData\\Roaming\\ASP.NET\\https\\store-client.pem',
            keyfile='C:\\Users\\allakn2\\AppData\\Roaming\\ASP.NET\\https\\store-client.pem',
            server_side=True
        )

def start_server(callback, use_https=False):
    global httpd
    MyHandler.callback = callback
    
    '''if use_https:
        httpd = StoppableHTTPSServer(('127.0.0.1', 443), MyHandler)
        print('Server listening on https://127.0.0.1:443/')        
    else:
        httpd = StoppableHTTPServer(('127.0.0.1', 8080), MyHandler)
        print('Server listening on http://127.0.0.1:80/')'''
        
    if use_https:
        httpd = StoppableHTTPSServer(('127.0.0.1', 443), MyHandler)
        print('Server listening on {url}:443/')        
    else:
        httpd = StoppableHTTPServer(('127.0.0.1', 8080), MyHandler)
        print('Server listening on {url}:8080/')
        
    # Start the server in a separate thread
    server_thread = threading.Thread(target=httpd.run)
    server_thread.start()

def stop_server():
    if httpd:
        print('Stopping the server...')
        httpd.shutdown()
        httpd.server_close()
        print('Server stopped.')

if __name__ == '__main__':
    start_server(use_https=False)

'''from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import TCPServer
from urllib.parse import parse_qs
import threading
import ssl
from custom_logger import *

class MyHandler(SimpleHTTPRequestHandler):
    callback : None
    def do_GET(self):
        auth_code = None
        query_string = self.path.split('?', 1)[-1]
        print(f"Received GET request with query string: {query_string}")
        # Parse the query string
        query_params = parse_qs(query_string)
        # Print the extracted query parameters
        for key, values in query_params.items():
            print(f"{key}: {values[0]}")
            if key == "auth_code" :
                if not auth_code:                    
                    auth_code = values[0]                

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Request received and processed successfully')
        if auth_code:
            self.callback(auth_code)
            stop_server()

class StoppableHTTPServer(HTTPServer):
    def run(self):
        self.serve_forever()
        
class StoppableHTTPSServer(StoppableHTTPServer):
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self.socket = ssl.wrap_socket(
            self.socket,
            certfile='C:\\Users\\allakn2\\AppData\\Roaming\\ASP.NET\\https\\store-client.pem',
            keyfile='C:\\Users\\allakn2\\AppData\\Roaming\\ASP.NET\\https\\store-client.pem',
            server_side=True
        )

def start_server(callback, use_https=False):
    global httpd
    MyHandler.callback = callback
    
    if use_https:
        httpd = StoppableHTTPSServer(('127.0.0.1', 443), MyHandler)
        print('Server listening on https://127.0.0.1:443/')        
    else:
        httpd = StoppableHTTPServer(('127.0.0.1', 80), MyHandler)
        print('Server listening on http://127.0.0.1:80/')
        
    # Start the server in a separate thread
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.start()

def stop_server():
    if httpd:
        print('Stopping the server...')
        httpd.shutdown()
        httpd.server_close()
        print('Server stopped.')
        
        

if __name__ == '__main__':
    start_server()
    '''
