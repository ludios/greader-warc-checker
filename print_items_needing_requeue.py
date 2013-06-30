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
	greader_items = sys.argv[1]
	basedirs = sys.argv[2:]
	assert basedirs, "Give me some basedirs containing .verification files"
	valids = set()
	invalids = set()
	largest = 0
	for basedir in basedirs:
		for directory, dirnames, filenames in os.walk(basedir):
			if basename(directory).startswith("."):
				continue

			for f in filenames:
				if f.startswith("."):
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
							largest = max(largest, int(data["item_name"], 10))

	for n in xrange(largest):
		item_name = str(n).zfill(10)
		if not item_name in valids and os.path.exists(greader_items + '/' + item_name[:6] + '/' + item_name + '.gz'):
			print item_name


if __name__ == '__main__':
	main()
