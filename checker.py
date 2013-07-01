#!/usr/bin/env python

__version__ = "20130701.1950"

import os
import sys
import time
import gzip
import re
import subprocess
import datetime
import random
import traceback
import zlib
import urllib2
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


def gunzip_string(s):
	return zlib.decompress(s, 16 + zlib.MAX_WBITS)


def get_expected_encoded_feed_urls(greader_items, item_name):
	text = None
	e = None

	for location in greader_items.split("|"):
		try:
			if location.startswith("http://"):
				text = gunzip_string(urllib2.urlopen(location + item_name[0:6] + '/' + item_name + '.gz').read())
			else:
				text = slurp_gz(join(location, item_name[0:6], item_name + '.gz'))
		except (OSError, IOError), e:
			continue
		else:
			break

	if text is None:
		raise RuntimeError("Could not get expected feed URLs for "
			"%r; tried %r; last error was %r" % (item_name, greader_items, e))

	return text.rstrip("\n").split("\n")


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
	WANT_FIRST_TARGET_URI, \
	NEED_SECOND_TARGET_URL, \
	NEED_STATUS_LINE, \
	WANT_CONTINUATION, \
	WANT_WGET_LOG_STATUS = range(5)

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
					raise BadWARC("End of file but did not get a status code for %r" % (last_url,))
				yield {"url": last_url, "continuation": continuation, "status_code": status_code}
			break

		if state == WANT_FIRST_TARGET_URI:
			if line.startswith(r'href\u003d\"'):
				hrefs.add(line[12:-3])
			# in this state, we may get the Target-URI, or we may get an href, or URL in wget log
			elif line.startswith("WARC-Target-URI: "):
				if last_url is not None:
					if status_code is None:
						raise BadWARC("Got next request but did not get a status code for last response %r" % (last_url,))
					yield {"url": last_url, "continuation": continuation, "status_code": status_code}
				continuation = None
				status_code = None
				last_url = line[17:-2]
				if last_url[:4] != "http": # skip warc metadata
					last_url = None
					state = WANT_FIRST_TARGET_URI
				else:
					state = NEED_SECOND_TARGET_URL
			elif line.startswith("https://www.google.com/reader/api/") and "&client=ArchiveTeam:" in line:
				last_url = line.rstrip()[:-1] # remove one ":"
				state = WANT_WGET_LOG_STATUS
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
			# Sometimes wget writes more than request to the WARC (does the first
			# time out?), so we may see a third (or fourth?) WARC-Target-URI: instead
			# of a status line.
			if line.startswith("WARC-Target-URI: ") and line[17:-2] == last_url:
				pass
				# state still NEED_STATUS_LINE
			else:
				try:
					http_version, status_code, message = line.split(" ", 2)
				except ValueError:
					raise BadWARC("Got unexpected status line %r" % (line,))
				if http_version not in ("HTTP/1.0", "HTTP/1.1") or status_code not in ("200", "404", "414", "400"):
					raise BadWARC("Got unexpected status line %r" % (line,))
				state = WANT_CONTINUATION

		elif state == WANT_CONTINUATION:
			# could get continuation (once chance at this), or a link, or next request, or URL in wget log
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
			elif line.startswith("https://www.google.com/reader/api/") and "&client=ArchiveTeam:" in line:
				last_url = line.rstrip()[:-1] # remove one ":"
				state = WANT_WGET_LOG_STATUS
			else:
				# Ignore unexpected lines, as the lack of ^ in our initial grep filter
				# outputs some garbage
				pass

		elif state == WANT_WGET_LOG_STATUS:
			try:
				error_string, code_string, rest = line.split(None, 2)
			except ValueError:
				raise BadWARC("wget log status line could not be split: %r" % (line,))
			status_code = code_string.rstrip(":")
			yield {"url": last_url, "continuation": None, "status_code": status_code}
			continuation = None
			status_code = None
			last_url = None
			state = WANT_FIRST_TARGET_URI

		else:
			raise RuntimeError("Invalid state %r" % (state,))


def get_info_from_warc_fname(fname):
	"""
	Where fname is absolute path, or at least includes the uploader parent dir
	"""
	uploader = basename(parent(fname))
	_, item_name, _, _ = basename(fname).split('-')
	return dict(uploader=uploader, item_name=item_name, basename=basename(fname))


def get_hrefs_fname(fname):
	assert fname.endswith(".warc.gz"), fname
	if fname.endswith(".cooked.warc.gz"):
		return fname.rsplit(".", 3)[0] + ".hrefs.bz2"
	else:
		return fname.rsplit(".", 2)[0] + ".hrefs.bz2"


def check_warc(fname, info, greader_items, href_log, reqres_log, exes):
	uploader = info['uploader']
	item_name = info['item_name']
	expected_urls = set(full_greader_url(efu) for efu in get_expected_encoded_feed_urls(greader_items, item_name))

	check_filename(fname)

	# We use pipes to allow for multi-core execution without writing a crazy amount
	# of Python code that wires up subprocesses.
	# Do not add a ^ to the grep - it will slow things 6x.
	extract_links = not os.path.exists(get_hrefs_fname(fname))
	if extract_links:
		keep_re = r'href\\u003d\\"[^\\]+\\"|"continuation":"C.{10}C"|WARC-Target-URI: .*|HTTP/1\.[01] ... .| ERROR 404: Not Found\.| ERROR 400: Bad Request\.| ERROR 414: Request-URI Too Large\.|https://www\.google\.com/reader/api/.*client=ArchiveTeam:'
		grep_flags = '-P'
	else:
		keep_re = r'''"continuation":"C.\{10\}C"\|HTTP/1\.[01] ... .\|WARC-Target-URI: .*\| ERROR 404: Not Found\.\| ERROR 400: Bad Request\.\| ERROR 414: Request-URI Too Large\.\|https://www\.google\.com/reader/api/.*client=ArchiveTeam:'''
		grep_flags = ''
	assert not "'" in keep_re
	args = [exes['sh'], '-c', r"""
trap '' INT tstp 30;
%(gunzip)s --to-stdout '%(fname)s' |
LC_LOCALE=C %(grep)s %(grep_flags)s --color=never -o '%(keep_re)s'""".replace("\n", "") % dict(
		fname=fname, keep_re=keep_re, grep_flags=grep_flags, **exes)]
	gunzip_grep_proc = subprocess.Popen(args, stdout=subprocess.PIPE, bufsize=4*1024*1024, close_fds=True)
	# TODO: do we need to read stderr continuously as well to avoid deadlock?
	try:
		found_hrefs = set()
		got_urls = set()
		# Note: if gzip file is corrupt, stdout will be empty and a BadWARC will be raised
		# complaining about the WARC missing every URL it was expected to have.
		for req_rep in read_request_responses(gunzip_grep_proc.stdout, found_hrefs):
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
	finally:
		_, stderr = gunzip_grep_proc.communicate()
		if stderr:
			print stderr
			raise BadWARC("Got stderr from gunzip-grep process: %r" % (stderr,))
		if gunzip_grep_proc.returncode != 0:
			raise BadWARC("Got process exit code %r from gunzip-grep process" % (gunzip_grep_proc.returncode,))

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


def has_hrefs_or_is_old(fname, seconds):
	assert os.path.exists(fname), fname
	return os.path.exists(get_hrefs_fname(fname)) or get_mtime(fname) < time.time() - seconds


def check_input_base(options, verified_dir, bad_dir, hrefs_dir, href_log, reqres_log, verification_log, exes, full_date):
	stopfile = join(os.getcwd(), "STOP")
	print "WARNING: To stop, do *not* use ctrl-c; instead, touch %s" % (stopfile,)
	initial_stop_mtime = get_mtime(stopfile)

	start = time.time()
	size_total = 0
	count = 0
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
			if fname.endswith('.warc.gz') and has_hrefs_or_is_old(fname, 600):
				count += 1
				if options.check_limit and count > options.check_limit:
					print "Stopping because --check-limit=%r was reached" % (options.check_limit,)
					return

				size_total += os.stat(fname).st_size
				def get_mb_sec():
					return ("%.2f MB/s" % (size_total/(time.time() - start) / (1024 * 1024))).rjust(10)

				info = get_info_from_warc_fname(fname)
				try:
					check_warc(fname, info, options.greader_items, href_log, reqres_log, exes)
				except BadWARC:
					msg = "bad"
					valid = False
					tb = traceback.format_exc()
					dest_dir = bad_dir
				else:
					msg = "ok "
					valid = True
					tb = None
					dest_dir = verified_dir

				print get_mb_sec(), msg, filename_without_prefix(fname, options.input_base)
				if verification_log:
					json.dump(dict(
						checker_version=__version__, valid=valid,
						traceback=tb, **info
					), verification_log)
					verification_log.write("\n")
					verification_log.flush()

				if dest_dir:
					dest_fname = join(dest_dir, filename_without_prefix(fname, options.input_base))
					try_makedirs(parent(dest_fname))
					os.rename(fname, dest_fname)

				if hrefs_dir and os.path.exists(get_hrefs_fname(fname)):
					full_date_hour = full_date[:13]
					hrefs_child_dir = join(hrefs_dir, full_date_hour)
					try_makedirs(hrefs_child_dir)
					os.rename(get_hrefs_fname(fname), join(hrefs_child_dir, full_date + '-' + info['item_name'] + '.hrefs.bz2'))


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
	parser.add_option('-g', "--greader-items", dest="greader_items", help="greader-items directory containing ./000000/0000000000.gz files.  (Needed to know which URLs we expect in a WARC.)  Can be a local directory or an http:// URI.  Separate multiple candidates with a pipe (|)")
	parser.add_option("-l", "--lists-dir", dest="lists_dir", help="Directory to write lists of status codes, bad items, new URLs to.")
	parser.add_option("-c", "--check-limit", dest="check_limit", type="int", default=None, help="Exit after checking this many items")

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
		hrefs_dir = join(options.output_base, "hrefs")
		try_makedirs(bad_dir)
	else:
		verified_dir = None
		bad_dir = None
		hrefs_dir = None

	exes = get_exes()

	now = datetime.datetime.now()
	full_date = now.isoformat().replace("T", "_").replace(':', '-') + "_" + str(random.random())[2:8]

	if options.lists_dir:
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
		check_input_base(options, verified_dir, bad_dir, hrefs_dir, href_log, reqres_log, verification_log, exes, full_date)
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
