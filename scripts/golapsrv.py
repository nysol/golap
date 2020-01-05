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
			cnf = golapM.getConf()
			try:
				jsonconf = json.dumps(cnf,ensure_ascii=False,indent=4)
				return Response("status:0\n{}\n".format(jsonconf),mimetype='text/plain' )
			except:
				return Response("status:-1\nFailed to convert json\n",mimetype='text/plain' )

		elif sjson['control'] == 'bye':
			golapM.save()
			return Response( "status:0\nserver state save\n",mimetype='text/plain' )

		else:
			return Response( "status:-1\nunknown control request\n",mimetype='text/plain' )

	# 'retrieve'
	elif 'retrieve' in sjson : 

		rtn = ""
		vlist = None

		try:
			kv  = sjson['retrieve'].split(",",2)
		
			if kv[0] == 'ListTraAtt':
				vlist = golapM.getTraFieldName()
				rtn += "TraAtt\n"
	
			elif kv[0] == 'GetTraAtt':
				if len(kv) <= 1:
					raise Exception("getTraAtt is nesseary field name")
				
				vlist = golapM.getTraFieldAtt(kv[1])
				rtn += "GetTraAtt\n"


			elif kv[0] == 'GetItmAtt':
				if len(kv) <= 1:
					raise Exception("getItmAtt is nesseary field name")

				itmfil = ""
				if 'itemFilter' in sjson :
					itmfil = sjson['itemFilter']
				
				vlist = golapM.getItmFieldAtt(kv[1],itmfil)
				rtn += "GetItmAtt\n"

			else:
				raise Exception("unknown retrieve request")

		except Exception as ep :
			return Response( "status:-1\n{}".format(str(ep)) , mimetype='text/plain' )

		for v in vlist:
			rtn += "{}\n".format(v)

		return Response( rtn , mimetype='text/plain' )

	# 'nodeimage'
	elif 'nodeimage' in sjson : 
		try:
			vlist = golapM.getNodeIMG(sjson)
		except Exception as ep :
			return Response( "status:-1\n{}".format(str(ep)) , mimetype='text/plain' )

		rtn = "status:0,sent:{size},hit:{size}\n{fldname}\n".format(size=str(len(vlist)),fldname=golapM.getImgFldName())
		for v in vlist:
			rtn += "{}\n".format(v)

		return Response( rtn ,mimetype='text/plain')

	# 'nodeimage'
	elif 'nodestat' in sjson : 
		try:
			vlist = golapM.getNodeStat(sjson)
		except Exception as ep :
			return Response( "status:-1\n{}".format(str(ep)) , mimetype='text/plain' )

		rtn = "status:0,sent:{size},hit:{size}\n".format(size=str(len(vlist)-1))
		for v in vlist:
			rtn += "{}\n".format(','.join(v))

		return Response( rtn ,mimetype='text/plain')

	elif 'query' in sjson : 

		try:
			rtnobj = golapM.query(sjson)

		except Exception as ep :
			return Response( "status:-1\n{}".format(str(ep)) , mimetype='text/plain' )

		rtn = ""
		if isinstance(rtnobj,list) :

			for vv in rtnobj:
				rtn += "%s:%s\n"%(vv["dmName"],vv["dmValue"])
				rtn += "status:%ld,sent:%ld,hit:%ld\n"%(vv["status"],vv["sent"],vv["hit"])
				rtn += "{}\n".format( ','.join(vv["header"]) )		
				for v in vv["data"]:
					rtn += "{}\n".format(','.join(v))
				rtn += "\n"

		else:

			rtn = "status:%ld,sent:%ld,hit:%ld\n"%(rtnobj["status"],rtnobj["sent"],rtnobj["hit"])
			rtn += "{}\n".format( ','.join(rtnobj["header"]) )		
			for v in rtnobj["data"]:
				rtn += "{}\n".format(','.join(v))

		return Response( rtn ,mimetype='text/plain')

	else:
		# query
		return Response("status:-1\nUnknown Request Pattern\n",mimetype='text/plain')


if __name__ == '__main__':

	app.run(host='0.0.0.0',port=useport,threaded=True)
	
	