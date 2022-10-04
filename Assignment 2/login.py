
from __future__ import print_function
import hashlib

if hasattr(__builtins__, 'raw_input'):
    input = raw_input
import sys
with open('answer.txt', 'w') as outfile:
	print("=====================")
	print("Engineering lab login")
	print("=====================")
	pw = input("Please enter the lab password: ")
	print('%s' % (pw,), file=outfile)
	if hashlib.md5(pw.encode('utf-8')).hexdigest() != '0603f637adc9a5bd85002f4a202f6fc7':
		sys.exit("Failed")
	ip = input("Please enter which IP to login to: ")
	print('%s' % (ip,), file=outfile)
	if hashlib.md5(ip.encode('utf-8')).hexdigest() != '41ec682a2319355d3df777115aa60d12':
		sys.exit("Invalid IP; exiting")
	action = input("Enter action (disarm|exit): ")
	if action != "disarm":
		sys.exit("Exiting")
	print("Success")
	