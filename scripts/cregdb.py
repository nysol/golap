#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import nysol.util.margs as margs
import nysol.take as nt
import nysolgolap._nysolgolap_core as ng

args=margs.Margs(sys.argv,"i=,mp=","i=")

ifn = args.str("i=")
mp = args.int("mp=",0)

if mp==0:
	ng.makeindex(ifn)
else:
	ng.makeindex(ifn,mp)


	
	