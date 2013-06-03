#!/usr/bin/env python

import os
import sys
import gzip
import re
import subprocess

from optparse import OptionParser

parent = os.path.dirname
basename = os.path.basename
join = os.path.join


def try_makedirs(p):
	try:
		os.makedirs(p)
	except OSError:
		pass


def slurp_gz(fname):
	f = gzip.open(fname, "rb")
	try:
		contents = f.read()
	finally:
		f.close()
	return contents


def get_expected_encoded_feed_urls(greader_items, item_name):
	return slurp_gz(join(greader_items, item_name[0:6], item_name + '.gz')).rstrip("\n").split("\n")


def full_greader_url(encoded_feed_url):
	return (
		"https://www.google.com/reader/api/0/stream/contents/feed/" +
		  encoded_feed_url +
		"?r=n&n=1000&hl=en&likes=true&comments=true&client=ArchiveTeam")


CONTINUATION_RE = re.compile(r"\?c=............&")
QUESTION_RE = re.compile(r"\?")

def url_with_continuation(url, continuation):
	assert len(continuation) == 12, "continuation should be 12 bytes, was %d" % (len(continuation),)
	if re.findall(CONTINUATION_RE, url):
		return re.sub(CONTINUATION_RE, "?c=" + continuation + "&", url)
	return re.sub(QUESTION_RE, "?c=" + continuation + "&", url, count=1)


def is_continued_url(url):
	return '?c=' in url


class BadWARC(Exception):
	pass



def read_request_responses(grepfh, hrefs):
	"""
	L{grepfh} is the file object containing grep-filtered WARC data.

	L{hrefs} is a set into which this will add JSON-encoded hrefs to.
	"""
	WANT_FIRST_TARGET_URI, NEED_SECOND_TARGET_URL, NEED_STATUS_LINE, WANT_CONTINUATION = range(4)
	state = WANT_FIRST_TARGET_URI
	last_url = None
	continuation = None
	status_code = None
	while True:
		line = grepfh.readline()
		if not line:
			if last_url is not None:
				if status_code is None:
					raise BadWARC("Did not get a status code for %r" % (last_url,))
				yield dict(url=last_url, continuation=continuation, status_code=status_code)
			break

		if state == WANT_FIRST_TARGET_URI:
			# in this state, we may get the Target-URI, or we may get an href
			if line.startswith("WARC-Target-URI: "):
				if last_url is not None:
					if status_code is None:
						raise BadWARC("Did not get a status code for %r" % (last_url,))
					yield dict(url=last_url, continuation=continuation, status_code=status_code)
				continuation = None
				status_code = None
				last_url = line[17:-2]
				if last_url[:4] != "http": # skip warc metadata
					last_url = None
					state = WANT_FIRST_TARGET_URI
				else:
					state = NEED_SECOND_TARGET_URL
			elif line.startswith(r'href\u003d\"'):
				hrefs.add(line[12:-3])
			else:
				# Ignore unexpected lines, as the lack of ^ in our initial grep filter
				# outputs some garbage
				pass

		elif state == NEED_SECOND_TARGET_URL:
			# Should be an exact duplicate of the last line
			assert last_url is not None, last_url
			if not line.startswith("WARC-Target-URI: "):
				raise BadWARC("Missing WARC-Target-URI for response")
			response_url = line[17:-2]
			if response_url != last_url:
				raise BadWARC("WARC-Target-URI for response did not match request: %r" % ((last_url, response_url),))
			state = NEED_STATUS_LINE

		elif state == NEED_STATUS_LINE:
			try:
				http_version, status_code, message = line.split(" ", 2)
			except ValueError:
				raise BadWARC("Got unexpected status line %r" % (line,))
			if http_version != "HTTP/1.1"or status_code not in ("200", "404"):
				raise BadWARC("Got unexpected status line %r" % (line,))
			state = WANT_CONTINUATION

		elif state == WANT_CONTINUATION:
			# could get continuation (once chance at this), or a link, or next request
			if line.startswith('"continuation":"'):
				continuation = line[16:28]
				state = WANT_FIRST_TARGET_URI
			elif line.startswith(r'href\u003d\"'):
				hrefs.add(line[12:-3])
				state = WANT_FIRST_TARGET_URI
			elif line.startswith("WARC-Target-URI: "):
				yield dict(url=last_url, continuation=continuation, status_code=status_code)
				continuation = None
				status_code = None
				last_url = line[17:-2]
				if last_url[:4] != "http": # skip warc metadata
					last_url = None
					state = WANT_FIRST_TARGET_URI
				else:
					state = NEED_SECOND_TARGET_URL
			else:
				# Ignore unexpected lines, as the lack of ^ in our initial grep filter
				# outputs some garbage
				pass

		else:
			raise RuntimeError("Invalid state %r" % (state,))


def check_warc(fname, greader_items):
	print fname

	uploader = basename(parent(fname))
	_, item_name, _, _ = basename(fname).split('-')
	expected_urls = set(full_greader_url(efu) for efu in get_expected_encoded_feed_urls(greader_items, item_name))

	assert not ' ' in fname, fname
	assert not "'" in fname, fname
	assert not "\\" in fname, fname

	# We use pipes to allow for multi-core execution without writing a crazy amount
	# of Python code that wires up subprocesses.

	# "Z8c8Jv5QWmpgVRxUsGoulMw" is the embedded 404 image we want to ignore

	# Do not add a ^ to the second grep - it will slow things 6x
	args = ['/bin/sh', '-c', r"""gunzip --to-stdout '%s' | grep -G --color=never -v "^Z8c8Jv5QWmpgVRxUsGoulMw" | grep -P --color=never -o 'href\\u003d\\"[^\\]+\\"|"continuation":"C.{10}C"|WARC-Target-URI: .*|HTTP/1\.1 .*'""" % (fname,)]
	proc = subprocess.Popen(args, stdout=subprocess.PIPE)
	found_hrefs = set()
	got_urls = set()
	for req_rep in read_request_responses(proc.stdout, found_hrefs):
		print req_rep
		url = req_rep['url']
		status_code = req_rep['status_code']
		if req_rep['continuation'] is not None:
			expected_urls.add(url_with_continuation(url, req_rep['continuation']))
		got_urls.add(url)
		if is_continued_url(url) and status_code != "200":
			raise BadWARC("All continued responses must be status 200, was %r" % (status_code,))

	if expected_urls != got_urls:
		raise BadWARC("WARC is missing %r or has extra URLs %r" % (expected_urls - got_urls, got_urls - expected_urls))

	sorted_hrefs = list(found_hrefs)
	sorted_hrefs.sort()
	##print "\n".join(repr(s) for s in sorted(found_hrefs))


def main():
	parser = OptionParser(usage="%prog [options]")

	parser.add_option("-i", "--input-base", dest="input_base", help="Base directory containing ./username/xxx.warc.gz files.")
	parser.add_option("-o", "--output-base", dest="output_base", help="Base directory to which to move input files; it will contain ./verified/username/xxx.warc.gz or ./bad-[failure mode]/username/xxx.warc.gz.  Should be on the same filesystem as --input-base.")
	parser.add_option('-g', "--greader-items", dest="greader_items", help="greader-items directory containing ./000000/0000000000.gz files.  (Needed to know which URLs we expect in a WARC.)")
	parser.add_option("-l", "--lists", dest="lists", help="Directory to write lists of status codes, bad items, new URLs to.")
	parser.add_option("-u", "--upload", dest="upload", help="rsync destination to sync lists to.")

	options, args = parser.parse_args()
	if not options.input_base or not options.output_base or not options.lists:
		print"--input-base, --output-base, --greader-items, and --lists are required"
		print
		parser.print_help()
		sys.exit(1)

	verified_dir = join(options.output_base, "verified")
	bad_status_code_dir = join(options.output_base, "bad-status-code")
	bad_missing_urls = join(options.output_base, "bad-missing-urls")
	bad_missing_continued_urls = join(options.output_base, "bad-missing-continued-urls")

	for directory, dirnames, filenames in os.walk(options.input_base):
		for f in filenames:
			fname = os.path.join(directory, f)
			if fname.endswith('.warc.gz'):
				try:
					check_warc(fname, options.greader_items)
				except BadWARC:
					# TODO move the file to bad/ instead
					raise


if __name__ == '__main__':
	main()
