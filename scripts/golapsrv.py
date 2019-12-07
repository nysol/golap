#!/usr/bin/env python

import sys
import json
import nysolgolap.golap as ngolap

if len(sys.argv) <= 1:
	print("useage:"+sys.argv[0] + " confFile" )
	exit()

confF = sys.argv[1]

with open(confF) as fpr:
	jconf = json.load(fpr)

useport = jconf['etc']['port']


golapM = ngolap.mgolap(confF)


from flask import Flask ,request , Response
app = Flask(__name__)

@app.route('/',methods=["POST"])
def reqpost():

	ss= request.get_data()
	try:
		sjson = json.loads(ss.decode())
	except:
		return Response("status:-1\njson parse error\n",mimetype='text/plain')

	if 'control' in sjson : 
		if sjson['control'] == 'config':
			return Response( "status:{sts}\n{rtn}\n".format(**golapM.getConf()),mimetype='text/plain' )
		elif sjson['control'] == 'bye':
			golapM.save()
			exit()
		else:
			return Response( "status:-1\nunknown control request\n",mimetype='text/plain' )
			

	return Response( golapM.query(json.dumps(sjson,ensure_ascii=False)),mimetype='text/plain')


if __name__ == '__main__':
	app.run(threaded=True,host='0.0.0.0',port=useport)
	
	