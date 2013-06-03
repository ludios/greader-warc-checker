#!/usr/bin/env python

__version__ = "20130603.2116"

import os
import sys
import time
import gzip
import re
import subprocess
import datetime
import random
import traceback
import distutils.spawn

from optparse import OptionParser

import json

parent = os.path.dirname
basename = os.path.basename
join = os.path.join


BAD_FNAME_RE = re.compile(r"""[\x00"'\\]+""")

def check_filename(fname):
	"""
	Raise L{ValueError} on any filename that can't be safely passed into a quoted
	shell command.
	"""
	if re.findall(BAD_FNAME_RE, fname):
		raise ValueError("Bad filename %r" % (fname,))


def filename_without_prefix(fname, prefix):
	if not fname.startswith(prefix + "/"):
		raise ValueError("%r does not start with %" % (fname, prefix + "/"))
	return fname.replace(prefix + "/", "", 1)


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
	_readline = grepfh.readline
	while True:
		line = _readline()
		if not line:
			if last_url is not None:
				if status_code is None:
					raise BadWARC("Did not get a status code for %r" % (last_url,))
				yield {"url": last_url, "continuation": continuation, "status_code": status_code}
			break

		if state == WANT_FIRST_TARGET_URI:
			if line.startswith(r'href\u003d\"'):
				hrefs.add(line[12:-3])
			# in this state, we may get the Target-URI, or we may get an href
			elif line.startswith("WARC-Target-URI: "):
				if last_url is not None:
					if status_code is None:
						raise BadWARC("Did not get a status code for %r" % (last_url,))
					yield {"url": last_url, "continuation": continuation, "status_code": status_code}
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
			if line.startswith(r'href\u003d\"'):
				hrefs.add(line[12:-3])
				state = WANT_FIRST_TARGET_URI
			elif line.startswith('"continuation":"'):
				continuation = line[16:28]
				state = WANT_FIRST_TARGET_URI
			elif line.startswith("WARC-Target-URI: "):
				yield {"url": last_url, "continuation": continuation, "status_code": status_code}
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


def get_info_from_warc_fname(fname):
	"""
	Where fname is absolute path, or at least includes the uploader parent dir
	"""
	uploader = basename(parent(fname))
	_, item_name, _, _ = basename(fname).split('-')
	return dict(uploader=uploader, item_name=item_name, basename=basename(fname))


def check_warc(fname, info, greader_items, href_log, reqres_log, exes):
	uploader = info['uploader']
	item_name = info['item_name']
	expected_urls = set(full_greader_url(efu) for efu in get_expected_encoded_feed_urls(greader_items, item_name))

	check_filename(fname)

	# We use pipes to allow for multi-core execution without writing a crazy amount
	# of Python code that wires up subprocesses.
	# Do not add a ^ to the grep - it will slow things 6x.
	keep_re = r'href\\u003d\\"[^\\]+\\"|"continuation":"C.{10}C"|WARC-Target-URI: .*|HTTP/1\.1 .*'
	assert not "'" in keep_re
	args = [exes['sh'], '-c', r"""
trap '' INT tstp 30;
%(gunzip)s --to-stdout '%(fname)s' |
%(grep)s -P --color=never -o '%(keep_re)s'""".replace("\n", "") % dict(
		fname=fname, keep_re=keep_re, **exes)]
	proc = subprocess.Popen(args, stdout=subprocess.PIPE, bufsize=4*1024*1024, close_fds=True)
	found_hrefs = set()
	got_urls = set()
	# Note: if gzip file is corrupt, stdout will be empty and a BadWARC will be raised
	# complaining about the WARC missing every URL it was expected to have.
	for req_rep in read_request_responses(proc.stdout, found_hrefs):
		req_rep_extra = dict(item_name=item_name, uploader=uploader, basename=info['basename'], **req_rep)
		##print json.dumps(req_rep_extra)
		if reqres_log:
			json.dump(req_rep_extra, reqres_log)
			reqres_log.write("\n")
		url = req_rep['url']
		status_code = req_rep['status_code']
		if req_rep['continuation'] is not None:
			expected_urls.add(url_with_continuation(url, req_rep['continuation']))
		got_urls.add(url)
		if is_continued_url(url) and status_code != "200":
			raise BadWARC("All continued responses must be status 200, was %r" % (status_code,))

	# Don't check for extra URLs - it looks like we're failing to detect a small
	# amount of continuation=s.
	# See ivan/greader-0000043504-20130531-171242.warc.gz
	# See ivan/greader-0000050357-20130601-005325.warc.gz

	##if expected_urls != got_urls:
	##	raise BadWARC("WARC is missing %r or has extra URLs %r" % (expected_urls - got_urls, got_urls - expected_urls))

	if expected_urls - got_urls:
		raise BadWARC("WARC is missing %r" % (expected_urls - got_urls,))

	sorted_hrefs = list(found_hrefs)
	sorted_hrefs.sort()
	if href_log is not None:
		for h in sorted_hrefs:
			href_log.write(h + "\n")


def get_mtime(fname):
	try:
		s = os.stat(fname)
	except OSError:
		return None
	return s.st_mtime


def check_input_base(options, verified_dir, bad_dir, href_log, reqres_log, verification_log, exes):
	stopfile = join(os.getcwd(), "STOP")
	print "WARNING: To stop, do *not* use ctrl-c; instead, touch %s" % (stopfile,)
	initial_stop_mtime = get_mtime(stopfile)

	start = time.time()
	size_total = 0
	for directory, dirnames, filenames in os.walk(options.input_base):
		if basename(directory).startswith("."):
			print "Skipping dotdir %r" % (directory,)
			continue

		for f in filenames:
			if get_mtime(stopfile) != initial_stop_mtime:
				print "Stopping because %s was touched" % (stopfile,)
				return

			if f.startswith("."):
				print "Skipping dotfile %r" % (f,)
				continue

			fname = os.path.join(directory, f)
			if fname.endswith('.warc.gz'):
				size_total += os.stat(fname).st_size
				def get_mb_sec():
					return ("%.2f MB/s" % (size_total/(time.time() - start) / (1024 * 1024))).rjust(10)

				info = get_info_from_warc_fname(fname)
				try:
					check_warc(fname, info, options.greader_items, href_log, reqres_log, exes)
				except BadWARC:
					print get_mb_sec(), "bad", filename_without_prefix(fname, options.input_base)
					if verification_log:
						json.dump(dict(
							checker_version=__version__, valid=False,
							traceback=traceback.format_exc(), **info
						), verification_log)
						verification_log.write("\n")
						verification_log.flush()

					if bad_dir:
						dest_fname = join(bad_dir, filename_without_prefix(fname, options.input_base))
						try_makedirs(parent(dest_fname))
						os.rename(fname, dest_fname)
				else:
					print get_mb_sec(), "ok ", filename_without_prefix(fname, options.input_base)
					if verification_log:
						json.dump(dict(
							checker_version=__version__, valid=True,
							traceback=None, **info
						), verification_log)
						verification_log.write("\n")
						verification_log.flush()

					if verified_dir:
						dest_fname = join(verified_dir, filename_without_prefix(fname, options.input_base))
						try_makedirs(parent(dest_fname))
						os.rename(fname, dest_fname)


def get_exes():
	bzip2_exe = distutils.spawn.find_executable('lbzip2')
	if not bzip2_exe:
		print "WARNING: Install lbzip2; this program is ~1.4x slower with vanilla bzip2"
		bzip2_exe = distutils.spawn.find_executable('bzip2')
		if not bzip2_exe:
			raise RuntimeError("lbzip2 or bzip2 not found in PATH")

	gunzip_exe = distutils.spawn.find_executable('gunzip')
	if not gunzip_exe:
		raise RuntimeError("gunzip not found in PATH")

	grep_exe = distutils.spawn.find_executable('grep')
	if not grep_exe:
		raise RuntimeError("grep not found in PATH")

	sh_exe = distutils.spawn.find_executable('sh')
	if not sh_exe:
		raise RuntimeError("sh not found in PATH")

	return dict(bzip2=bzip2_exe, gunzip=gunzip_exe, grep=grep_exe, sh=sh_exe)


def main():
	parser = OptionParser(usage="%prog [options]")

	parser.add_option("-i", "--input-base", dest="input_base", help="Base directory containing ./username/xxx.warc.gz files.")
	parser.add_option("-o", "--output-base", dest="output_base", help="Base directory to which to move input files; it will contain ./verified/username/xxx.warc.gz or ./bad/username/xxx.warc.gz.  Should be on the same filesystem as --input-base.")
	parser.add_option('-g', "--greader-items", dest="greader_items", help="greader-items directory containing ./000000/0000000000.gz files.  (Needed to know which URLs we expect in a WARC.)")
	parser.add_option("-l", "--lists-dir", dest="lists_dir", help="Directory to write lists of status codes, bad items, new URLs to.")

	options, args = parser.parse_args()
	if not options.input_base or  not options.greader_items:
		print "--input-base and --greader-items are required"
		print
		parser.print_help()
		sys.exit(1)

	if not options.output_base:
		print "--output-base not specified; files in --input-base will not be moved"

	if not options.lists_dir:
		print "--lists-dir not specified; no lists will be written"

	if options.output_base:
		verified_dir = join(options.output_base, "verified")
		try_makedirs(verified_dir)
		bad_dir = join(options.output_base, "bad")
		try_makedirs(bad_dir)
	else:
		verified_dir = None
		bad_dir = None

	exes = get_exes()

	if options.lists_dir:
		now = datetime.datetime.now()
		full_date = now.isoformat().replace("T", "_").replace(':', '-') + "_" + str(random.random())[2:8]

		href_log_fname = join(options.lists_dir, full_date + ".hrefs.bz2")
		check_filename(href_log_fname)
		assert not os.path.exists(href_log_fname), href_log_fname
		try_makedirs(parent(href_log_fname))
		# trap '' INT tstp 30; prevents sh from catching SIGINT (ctrl-c).  If untrapped,
		# bzip2 or lbzip2 will be killed when you hit ctrl-c, leaving you with a corrupt .bz2.
		# (Sadly, this `trap` does not seem to work with lbzip2.)
		#
		# We use close_fds=True because otherwise .communicate() later deadlocks on
		# Python 2.6.  See http://stackoverflow.com/questions/14615462/why-does-communicate-deadlock-when-used-with-multiple-popen-subprocesses
		href_log_proc = subprocess.Popen(
			[exes['sh'], '-c', r"trap '' INT tstp 30; %(bzip2)s > %(href_log_fname)s" %
						dict(href_log_fname=href_log_fname, **exes)],
			stdin=subprocess.PIPE, bufsize=4*1024*1024, close_fds=True)
		href_log = href_log_proc.stdin

		reqres_log_fname = join(options.lists_dir, full_date + ".reqres.bz2")
		check_filename(reqres_log_fname)
		assert not os.path.exists(reqres_log_fname), reqres_log_fname
		try_makedirs(parent(reqres_log_fname))
		reqres_log_proc = subprocess.Popen(
			[exes['sh'], '-c', r"trap '' INT tstp 30; %(bzip2)s > %(reqres_log_fname)s" %
				dict(reqres_log_fname=reqres_log_fname, **exes)],
			stdin=subprocess.PIPE, bufsize=4*1024*1024, close_fds=True)
		reqres_log = reqres_log_proc.stdin

		# Don't use bzip for this one; we want to flush it line by line
		verification_log_fname = join(options.lists_dir, full_date + ".verification")
		assert not os.path.exists(verification_log_fname), verification_log_fname
		try_makedirs(parent(verification_log_fname))
		verification_log = open(verification_log_fname, "wb")
	else:
		href_log = None
		reqres_log = None
		verification_log = None

	try:
		check_input_base(options, verified_dir, bad_dir, href_log, reqres_log, verification_log, exes)
	finally:
		if href_log is not None:
			href_log.close()
			_, stderr = href_log_proc.communicate()
			if stderr:
				print stderr
		if reqres_log is not None:
			reqres_log.close()
			_, stderr = reqres_log_proc.communicate()
			if stderr:
				print stderr
		if verification_log is not None:
			verification_log.close()


if __name__ == '__main__':
	main()
