#!/usr/bin/env python

import os
import sys
import gzip
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


def check_warc(fname, greader_items):
	print fname

	uploader = basename(parent(fname))
	_, item_name, _, _ = basename(fname).split('-')
	expected_encoded_feed_urls = slurp_gz(join(greader_items, item_name[0:6], item_name + '.gz')).rstrip("\n").split("\n")
	expected_urls = list(full_greader_url(efu) for efu in expected_encoded_feed_urls)

	# We use pipes to allow for multi-core execution without writing a crazy amount
	# of Python code.

	# "Z8c8Jv5QWmpgVRxUsGoulMw" is the embedded 404 image we want to ignore
	# "        " is what begins styling on the 404 page

	assert not ' ' in fname, fname
	assert not "'" in fname, fname
	assert not "\\" in fname, fname
	args = ['/bin/sh', '-c', r"""gunzip --to-stdout '%s' | grep -P --color=never -v "^(Z8c8Jv5QWmpgVRxUsGoulMw|        )" | grep -P --color=never -o 'href\\u003d\\"[^\\]+\\"|"continuation":"C.{10}C"|WARC-Target-URI: .*|HTTP/1\.1 .*'""" % (fname,)]
	proc = subprocess.Popen(args, stdout=subprocess.PIPE)
	while True:
		line = proc.stdout.readline()
		if not line:
			break
		print line.rstrip()


def main():
	parser = OptionParser(usage="%prog [options]")

	parser.add_option("-i", "--input-base", dest="input_base", help="Base directory containing ./username/xxx.warc.gz files.")
	parser.add_option("-o", "--output-base", dest="output_base", help="Base directory to which to move input files; it will contain ./verified/username/xxx.warc.gz or ./bad-[failure mode]/username/xxx.warc.gz.  Should be on the same filesystem as --input-base.")
	parser.add_option('-g', "--greader-items", dest="greader_items", help="greader-items directory containing ./000000/0000000000.gz files.  (Needed to know which URLs we expect in a WARC.)")
	parser.add_option("-l", "--lists", dest="lists", help="Directory to write lists of status codes, bad items, new URLs to.")

	options, args = parser.parse_args()
	if not options.input_base or not options.output_base or not options.lists:
		print"--input-base, --output-base, --greader-items, and --lists are required"
		print
		parser.print_help()
		sys.exit(1)

	for directory, dirnames, filenames in os.walk(options.input_base):
		for f in filenames:
			fname = os.path.join(directory, f)
			if fname.endswith('.warc.gz'):
				check_warc(fname, options.greader_items)


if __name__ == '__main__':
	main()
