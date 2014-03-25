from xmlrpclib import ServerProxy 

if __name__ == "__main__":
    server = ServerProxy("http://localhost:8080") 
    print server.add(2)
    server.kill()