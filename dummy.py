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


def check_warc(fname, greader_items):
	print fname

	uploader = basename(parent(fname))
	_, item_name, _, _ = basename(fname).split('-')
	expected_encoded_feed_urls = slurp_gz(join(greader_items, item_name[0:6], item_name + '.gz')).rstrip("\n").split("\n")
	expected_urls = list(full_greader_url(efu) for efu in expected_encoded_feed_urls)

	links = set()

	assert not ' ' in fname, fname
	assert not "'" in fname, fname
	assert not "\\" in fname, fname

	# We use pipes to allow for multi-core execution without writing a crazy amount
	# of Python code that wires up subprocesses.

	# "Z8c8Jv5QWmpgVRxUsGoulMw" is the embedded 404 image we want to ignore

	# Do not add a ^ to the second grep - it will slow things 6x
	args = ['/bin/sh', '-c', r"""gunzip --to-stdout '%s' | grep -G --color=never -v "^Z8c8Jv5QWmpgVRxUsGoulMw" | grep -P --color=never -o 'href\\u003d\\"[^\\]+\\"|"continuation":"C.{10}C"|WARC-Target-URI: .*|HTTP/1\.1 .*'""" % (fname,)]
	proc = subprocess.Popen(args, stdout=subprocess.PIPE)
	last_url = None
	found_urls = set()
	while True:
		line = proc.stdout.readline()
		if not line:
			break
		##print line
		if line.startswith(r'href\u003d\"'):
			links.add(line[12:-3])
		elif line.startswith("WARC-Target-URI: "):
			#print line
			last_url = line[17:-1]
			found_urls.add(last_url)
		elif line.startswith("HTTP/1.1 "):
			_, status_code, message = line.split(" ", 2)
			# last_url

			# if code not in ("200", "404"):
		elif line.startswith('"continuation":"'):
			continuation = line[16:28]
			if last_url:
				print url_with_continuation(last_url, continuation)
			# TODO: add to expected_urls, or another list/set
			#1/0
		else:
			# Ignore
			pass
		#elif line.startswith('')

	##print "\n".join(sorted(links))


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
				check_warc(fname, options.greader_items)


if __name__ == '__main__':
	main()
