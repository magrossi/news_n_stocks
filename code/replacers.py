# -*- coding: utf-8 -*-
import re, string

##################################################
## Replacing Words Matching Regular Expressions ##
##################################################

replacement_patterns = [
	(u'won[\'\u2018\u2019]t', 'will not'),
	(u'can[\'\u2018\u2019]t', 'cannot'),
	(u'i[\'\u2018\u2019]m', 'i am'),
	(u'ain[\'\u2018\u2019]t', 'is not'),
	(u'(\w+)[\'\u2018\u2019]ll', '\g<1> will'),
	(u'(\w+)n[\'\u2018\u2019]t', '\g<1> not'),
	(u'(\w+)[\'\u2018\u2019]ve', '\g<1> have'),
	(u'[\u201c\u201d\"]', ''),
	(u'(\w+)[\'\u2018\u2019]s', '\g<1> is'),
	(u'(\w+)[\'\u2018\u2019]re', '\g<1> are'),
	(u'(\w+)[\'\u2018\u2019]d', '\g<1> would'),
	(ur'[{}]'.format(string.punctuation), ' '),
]

class RegexpReplacer(object):
	""" Replaces regular expression in a text.
	>>> replacer = RegexpReplacer()
	>>> replacer.replace("can't is a contraction")
	'cannot is a contraction'
	>>> replacer.replace("I should've done that thing I didn't do")
	'I should have done that thing I did not do'
	"""
	def __init__(self, patterns=replacement_patterns):
		self.patterns = [(re.compile(regex), repl) for (regex, repl) in patterns]

	def replace(self, text):
		s = text

		for (pattern, repl) in self.patterns:
			s = re.sub(pattern, repl, s)

		return s
