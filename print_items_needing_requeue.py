"""
Walks through your greader-logs directory (or directory containing them)
and prints every item_name that has been finished but has no valid .warc.gz
(as determined by greader-warc-checker's .verification logs)
"""

import os
import sys
try:
	import simplejson as json
except ImportError:
	import json

basename = os.path.basename

def main():
	basedirs = sys.argv[1:]
	valids = set()
	invalids = set()
	for basedir in basedirs:
		for directory, dirnames, filenames in os.walk(basedir):
			if basename(directory).startswith("."):
				print "Skipping dotdir %r" % (directory,)
				continue

			for f in filenames:
				if f.startswith("."):
					print "Skipping dotfile %r" % (f,)
					continue

				fname = os.path.join(directory, f)

				if fname.endswith(".verification"):
					with open(fname, "rb") as fh:
						for line in fh:
							data = json.loads(line)
							if data["valid"]:
								valids.add(data["item_name"])
							else:
								invalids.add(data["item_name"])

	needs_requeue = sorted(invalids - valids)
	for item_name in needs_requeue:
		print item_name


if __name__ == '__main__':
	main()
