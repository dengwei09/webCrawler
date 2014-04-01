# encoding: utf-8
import gzip

class Opener(object):
    def open(self, url):
        raise NotImplementedError
    
    def ungzip(self, fileobj):
        gz = gzip.GzipFile(fileobj=fileobj, mode='rb')
        try:
            return gz.read()
        finally:
            gz.close()