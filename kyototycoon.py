#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (c) 2013 Ulrich Mierendorff

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
'''

import socket
import struct

MB_SET_BULK = 0xb8
MB_GET_BULK = 0xba
MB_REMOVE_BULK = 0xb9
MB_ERROR = 0xbf
MB_PLAY_SCRIPT = 0xb4

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 1978
DEFAULT_EXPIRE = 0xffffffffff

FLAG_NOREPLY = 0x01

class KyotoTycoonError(Exception):
    """ Class for Exceptions in this module """

class KyotoTycoon:

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, lazy=True,
                 timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        if not lazy:
            self._connect()
    
    
    def set(self, key, val, db, expire=DEFAULT_EXPIRE, flags=0):
        return self.set_bulk(((key,val,db,expire),), flags)
    
    
    def set_bulk_kv(self, kv, db, expire=DEFAULT_EXPIRE, flags=0):
        recs = ((key,val,db,expire) for key,val in kv.iteritems())
        return self.set_bulk(recs, flags)
    
    
    def set_bulk(self, recs, flags=0):
        if self.socket is None:
            self._connect()
            
        request = [struct.pack('!BI', MB_SET_BULK, flags), None]
        
        cnt = 0
        for key,val,db,xt in recs:
            request.append(struct.pack('!HIIq', db, len(key), len(val), xt))
            request.append(key)
            request.append(val)
            cnt += 1
        
        request[1] = struct.pack('!I', cnt)
        
        self._write(''.join(request))
        
        if flags & FLAG_NOREPLY:
            return None
            
        magic, = struct.unpack('!B', self._read(1))
        if magic == MB_SET_BULK:
            recs_cnt, = struct.unpack('!I', self._read(4))
            return recs_cnt
        elif magic == MB_ERROR:
            raise KyotoTycoonError('Internal server error 0x%02x' % MB_ERROR)
        else:
            raise KyotoTycoonError('Unknown server error')
    
    
    def get(self, key, db, flags=0):
        recs = self.get_bulk(((key, db),), flags)
        if not recs:
            return None
        return recs[0][1]
    
    
    def get_bulk_keys(self, keys, db, flags=0):
        recs = ((key,db) for key in keys)
        recs = self.get_bulk(recs, flags)
        return dict(((key,val) for key,val,db,xt in recs))
    
    
    def get_bulk(self, recs, flags=0):
        if self.socket is None:
            self._connect()
            
        request = [struct.pack('!BI', MB_GET_BULK, flags), None]
        
        cnt = 0
        for key,db in recs:
            request.append(struct.pack('!HI', db, len(key)))
            request.append(key)
            cnt += 1
        
        request[1] = struct.pack('!I', cnt)
        
        self._write(''.join(request))
            
        magic, = struct.unpack('!B', self._read(1))
        if magic == MB_GET_BULK:
            recs_cnt, = struct.unpack('!I', self._read(4))
            recs = []
            for i in range(recs_cnt):
                db,key_len,val_len,xt = struct.unpack('!HIIq', self._read(18))
                key = self._read(key_len)
                val = self._read(val_len)
                recs.append((key,val,db,xt))
            return recs
        elif magic == MB_ERROR:
            raise KyotoTycoonError('Internal server error 0x%02x' % MB_ERROR)
        else:
            raise KyotoTycoonError('Unknown server error')
    
    
    
    def remove(self, key, db, flags=0):
        return self.remove_bulk(((key,db),), flags)
    
    
    def remove_bulk_keys(self, keys, db, flags=0):
        recs = ((key,db) for key in keys)
        return self.remove_bulk(recs, flags)
    
    
    def remove_bulk(self, recs, flags=0):
        if self.socket is None:
            self._connect()
            
        request = [struct.pack('!BI', MB_REMOVE_BULK, flags), None]
        
        cnt = 0
        for key,db in recs:
            request.append(struct.pack('!HI', db, len(key)))
            request.append(key)
            cnt += 1
        
        request[1] = struct.pack('!I', cnt)
        
        self._write(''.join(request))
        
        if flags & FLAG_NOREPLY:
            return None
            
        magic, = struct.unpack('!B', self._read(1))
        if magic == MB_REMOVE_BULK:
            recs_cnt, = struct.unpack('!I', self._read(4))
            return recs_cnt
        elif magic == MB_ERROR:
            raise KyotoTycoonError('Internal server error 0x%02x' % MB_ERROR)
        else:
            raise KyotoTycoonError('Unknown server error')
    
    
    def play_script(self, name, recs, flags=0):
        if self.socket is None:
            self._connect()
            
        request = [struct.pack('!BII', MB_PLAY_SCRIPT, flags, len(name)), None,
                    name]
        
        cnt = 0
        for key,val in recs:
            request.append(struct.pack('!II', len(key), len(val)))
            request.append(key)
            request.append(val)
            cnt += 1
        
        request[1] = struct.pack('!I', cnt)
        
        self._write(''.join(request))
        
        if flags & FLAG_NOREPLY:
            return None
            
        magic, = struct.unpack('!B', self._read(1))
        if magic == MB_PLAY_SCRIPT:
            recs_cnt, = struct.unpack('!I', self._read(4))
            recs = []
            for i in range(recs_cnt):
                key_len,val_len = struct.unpack('!II', self._read(8))
                key = self._read(key_len)
                val = self._read(val_len)
                recs.append((key,val))
            return recs
        elif magic == MB_ERROR:
            raise KyotoTycoonError('Internal server error 0x%02x' % MB_ERROR)
        else:
            raise KyotoTycoonError('Unknown server error')
    
    
    def close(self):
        if self.socket is not None:
            self.socket.close()
            self.socket = None
    
    
    def _connect(self):
        self.socket = socket.create_connection((self.host, self.port),
                                                self.timeout)
    
    
    def _write(self, data):
        self.socket.sendall(data)
        
    
    def _read(self, bytecnt):
        buf = []
        read = 0
        while read < bytecnt:
            recv = self.socket.recv(bytecnt-read)
            if recv:
                buf.append(recv)
                read += len(recv)
        
        return ''.join(buf)


