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
	sjson = json.loads(ss)
	return Response( golapM.query(json.dumps(sjson)),mimetype='text/plain')


if __name__ == '__main__':
	app.run(threaded=True,host='0.0.0.0',port=useport)
	
	