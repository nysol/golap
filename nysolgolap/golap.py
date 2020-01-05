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
		


	def getTraFieldName(self):

		flds =[]
		if 'traAttFile' in self.jsondf:
			for fldname in ['numFields','strFields']:
				if fldname in self.jsondf['traAttFile']:
					flds.extend(self.jsondf['traAttFile'][fldname].split(','))

		flds.sort()
		return flds


	def getTraFieldAtt(self,q):
	
		vlist = ng.getTraAtt(self.golapOBJ,q)

		# エラー
		if isinstance(vlist,dict) :
			raise Exception(vlist["errmsg"])		
		
		return vlist


	def getItmFieldAtt(self,q,ifil=""):

		if ifil == None or ifil == "" :
			vlist = ng.getItmAtt(self.golapOBJ,q)
		else:
			vlist = ng.getItmAtt(self.golapOBJ,q,ifil)

		# エラー
		if isinstance(vlist,dict) :
			raise Exception(vlist["errmsg"])

		return vlist


	def getNodeIMG(self,q):

		if not 'itemAttFile' in  self.jsondf:
			raise Exception("not found itemAtt")

		if not 'imageField' in self.jsondf['itemAttFile']:
			raise Exception("not found imageField")

		vlist = ng.getNodeIMG(self.golapOBJ,q)

		# エラー
		if isinstance(vlist,dict) :
			raise Exception(vlist["errmsg"])
		
		return vlist


	def getNodeStat(self,q):

		vlist = ng.getNodeStat(self.golapOBJ,q)

		# エラー
		if isinstance(vlist,dict) :
			raise Exception(vlist["errmsg"])
			
		return vlist


	def query(self,q):

		rtnobj = ng.run(self.golapOBJ,q)		

		if isinstance(rtnobj,list) :
			for vv in rtnobj:
				if vv["status"] == -1 :
					raise Exception(vv["errmsg"])
		
		else:
			if rtnobj["status"] == -1 :
				raise Exception(rtnobj["errmsg"])
			
		return rtnobj
		


		if isinstance(rtnobj,list) :
			rtn = ""
			for vv in rtnobj:
				rtn += "%s:%s\n"%(vv["dmName"],vv["dmValue"])

				rtn += "status:%ld,sent:%ld,hit:%ld\n"%(vv["status"],vv["sent"],vv["hit"])
				rtn += "{}\n".format( ','.join(vv["header"]) )		
				for v in vv["data"]:
					rtn += "{}\n".format(','.join(v))

				rtn += "\n"
		else:
			if rtnobj["status"] == -1 :
				rtn = "status:-1\n%s\n"%(rtnobj["errmsg"])
			
			else:
				rtn = "status:%ld,sent:%ld,hit:%ld\n"%(rtnobj["status"],rtnobj["sent"],rtnobj["hit"])
				rtn += "{}\n".format( ','.join(rtnobj["header"]) )		
				for v in rtnobj["data"]:
					rtn += "{}\n".format(','.join(v))

		return rtn


	def save(self):
		return ng.save(self.golapOBJ)
	
	def getConf(self):
		return self.jsondf

	def getImgFldName(self):

		if 'itemAttFile' in self.jsondf and 'imageField' in self.jsondf['itemAttFile'] :
			return self.jsondf['itemAttFile']['imageField']

		return ""


def makeIdx(confF,mp=0):

	if not os.path.isfile(confF): 
		raise Exception("FILE Not FOUND " + confF)
	
	if mp==0:
		ng.makeindex(confF)
	else:
		ng.makeindex(confF,mp)
	
	return mgolap(confF)

