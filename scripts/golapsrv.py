#!/usr/bin/env python

import sys
import os
import json
import time
import nysolgolap.golap as ngolap

if len(sys.argv) <= 1:
	print("useage:"+sys.argv[0] + " confFile" )
	exit()

confF = sys.argv[-1]

with open(confF) as fpr:
	jconf = json.load(fpr)

useport = jconf['etc']['port']
logD = jconf["dbDir"]+"/log"
if 'logDir' in jconf:
	logD = jconf['logDir']
	
os.makedirs(logD, exist_ok=True)


golapM = ngolap.mgolap(confF)
from flask import Flask ,request , Response
app = Flask(__name__)


@app.route('/',methods=["POST"])
def reqpost():

	ss= request.get_data()
	utstr = str(time.time())
	with open(logD+"/"+utstr,"w") as wfp:
		wfp.write(ss.decode())
		
	try:
		sjson = json.loads(ss.decode())
	except:
		return Response("status:-1\njson parse error\n",mimetype='text/plain')
	print(sjson)
	# control
	if 'control' in sjson : 
		if sjson['control'] == 'config':
			return Response( "status:{sts}\n{rtn}\n".format(**golapM.getConf()),mimetype='text/plain' )
		elif sjson['control'] == 'bye':
			golapM.save()
			exit()
		else:
			return Response( "status:-1\nunknown control request\n",mimetype='text/plain' )

	# 'retrieve'
	elif 'retrieve' in sjson : 
		
		kv     = sjson['retrieve'].split(",",2)
		itmfil = ""
		if 'itemFilter' in sjson :
			itmfil = sjson['itemFilter']

		if kv[0] == 'ListTraAtt':
			return Response( golapM.getTraFieldName(),mimetype='text/plain' )

		elif kv[0] == 'GetTraAtt':
			if len(kv) <= 1:
				return Response( "status:-1\ngetTraAtt is nesseary field name\n",mimetype='text/plain' )

			return Response( golapM.getTraFieldAtt(kv[1]),mimetype='text/plain' )

		elif kv[0] == 'GetItmAtt':
			if len(kv) <= 1:
				return Response( "status:-1\ngetItmAtt is nesseary field name\n",mimetype='text/plain' )

			return Response( golapM.getItmFieldAtt(kv[1],itmfil),mimetype='text/plain' )

		else:
			return Response( "status:-1\nunknown retrieve request\n",mimetype='text/plain' )

	# 'nodeimage'
	elif 'nodeimage' in sjson : 
		return Response( golapM.getNodeIMG(sjson),mimetype='text/plain')

	# 'nodeimage'
	elif 'nodestat' in sjson : 
		return Response( golapM.getNodeStat(sjson),mimetype='text/plain')

	elif 'query' in sjson : 
		return Response( golapM.query(sjson),mimetype='text/plain')
		
	else:
		# query
		return Response("status:-1\nUnknown Request Pattern\n",mimetype='text/plain')
		#return Response( golapM.query(json.dumps(sjson,ensure_ascii=False)),mimetype='text/plain')


if __name__ == '__main__':

	app.run(host='0.0.0.0',port=useport,threaded=True)
	
	