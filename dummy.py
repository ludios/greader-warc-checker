#!/usr/bin/env python
"""warcdump - dump warcs in a slightly more humane format"""

import os
import re
import sys

import sys
import os.path

from optparse import OptionParser

from hanzo.warctools import WarcRecord, expand_files
from hanzo.httptools import RequestMessage, ResponseMessage

parser = OptionParser(usage="%prog [options] warc warc warc")

parser.add_option("-l", "--limit", dest="limit")
parser.add_option("-I", "--input", dest="input_format")
parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(output_directory=None, limit=None, log_level="info")

def main(argv):
	(options, input_files) = parser.parse_args(args=argv[1:])

	for name in expand_files(input_files):
		fh = WarcRecord.open_archive(name, gzip="auto", mode="rb")
		dump_archive(fh,name)

		fh.close()

	return 0

class BadHTTPResponse(Exception):
	pass

# Based on warc-tools/hanzo/warclinks.py
def parse_http_response(record):
	message = ResponseMessage(RequestMessage())
	remainder = message.feed(record.content[1])
	message.close()
	if remainder or not message.complete():
		if remainder:
			raise BadHTTPResponse('trailing data in http response for %s' % (record.url,))
		if not message.complete():
			raise BadHTTPResponse('truncated http response for %s' % (record.url,))

	header = message.header

	mime_type = [v for k, v in header.headers if k.lower() == 'content-type']
	if mime_type:
		mime_type = mime_type[0].split(';', 1)[0]
	else:
		mime_type = None

	return header.code, mime_type, message

def dump(record, content=True):
	print 'Headers:'
	for h, v in record.headers:
		print '\t%s: %s' % (h, v)
	if content and record.content:
		print 'Content Headers:'
		content_type, content_body = record.content
		print '\t', record.CONTENT_TYPE + ':', content_type
		print '\t', record.CONTENT_LENGTH + ':', len(content_body)
		if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):
			status_code, mime_type, message = parse_http_response(record)
			print status_code
			print message.get_body()
		print
	else:
		print 'Content: none'
		print
		print
	if record.errors:
		print 'Errors:'
		for e in record.errors:
			print '\t', e

def dump_archive(fh, name, offsets=True):
	for offset, record, errors in fh.read_records(limit=None, offsets=offsets):
		if record:
			print "archive record at %s:%s" % (name, offset)
			#record.dump(content=True)
			#print record.get_header("Status")
			#print record.headers
			dump(record)
		elif errors:
			print "warc errors at %s:%d" % (name, offset if offset else 0)
			for e in errors:
				print '\t', e
		else:
			print
			print 'note: no errors encountered in tail of file'

def run():
	sys.exit(main(sys.argv))


if __name__ == '__main__':
	run()
