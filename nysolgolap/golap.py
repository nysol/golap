import os
import json
import nysolgolap._nysolgolap_core as ng

class mgolap(object):

	def __init__(self,confF):
		if not os.path.isfile(confF): 
			raise Exception("FILE Not FOUND " + confF)
		
		#json読み込み
		with open(confF) as f:
			self.jsondf = json.load(f)

		self.golapOBJ = ng.init(confF)
		ng.load(self.golapOBJ)
		

	def query(self,q):
		return ng.run(self.golapOBJ,q)

	def save(self,q):
		return ""
	
	def getConf(self):
		try:
			return { "sts":0 , "rtn" : json.dumps(self.jsondf,ensure_ascii=False,indent=4) }
		except:
			return { "sts":-1, "rtn" : "Failed to convert json" }
		
