#!/usr/bin/python
import socket
import time

a = socket.socket()
a.connect(('localhost', 1200))
a.setblocking(0)
#a.send('set notexist 0 0 6 noreply\r\nbarva2\r\n')
a.send('set notexist 0 0 2\r\nba\r\n')
#time.sleep(0.1)
#print a.recv(1024)
a.send('add notexis1 0 0 2\r\nba\r\n')
time.sleep(0.1)
print a.recv(1024)
a.send('get notexist notexis1\r\n')
time.sleep(0.1)
print a.recv(1024)
