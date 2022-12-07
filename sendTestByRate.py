#!/usr/bin/python
import random
import numpy
import sys
from time import sleep

# gan co de phan biet da doc het file chua va lap lai
exit=0 
# so dong can thuc hien
numberOfLines=int(sys.argv[1])
# file du lieu can gui den chuong trinh
fileToSend=sys.argv[2]
with open (fileToSend, 'r') as test:
	while True:
			for i in range(numberOfLines):
				line = test.readline()
				if not line:
				exit=1
				break
			# xoa cac khoang trong trong line
			print line.rstrip()
			sys.stdout.flush()
			if exit==1:
			# doi con tro ve dau van ban
			test.seek(0)
			sleep(1)