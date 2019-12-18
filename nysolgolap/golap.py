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

		rtn = "TraAtt\n"
		flds.sort()
		for v in flds:
			rtn += "{}\n".format(v)
			
		return rtn


	def getTraFieldAtt(self,q):
	
		vlist = ng.getTraAtt(self.golapOBJ,q)
		print(vlist)

		if isinstance(vlist,dict) :
			rtn = "status:%s\n%s\n"%(vlist["status"],vlist["errmsg"])
		
		else:
			rtn = "GetTraAtt\n"
			for v in vlist:
				rtn += "{}\n".format(v)
		
		return rtn


	def getNodeIMG(self,q):
		# ERRORの返す方法考える
		vlist = ng.getNodeIMG(self.golapOBJ,q)

		if isinstance(vlist,dict) :
			rtn = "status:%s\n%s\n"%(vlist["status"],vlist["errmsg"])
		
		else:
			rtn = "status:0,sent:{size},hit:{size}\n{fldname}\n".format(size=str(len(vlist)),fldname=self.jsondf['itemAttFile']['imageField'])
			rtn += "\n".join(vlist)

		return rtn

	def getNodeStat(self,q):
		# ERRORの返す方法考える
		vlist = ng.getNodeStat(self.golapOBJ,q)

		if isinstance(vlist,dict) :
			rtn = "status:%s\n%s\n"%(vlist["status"],vlist["errmsg"])
		else:
			rtn = "status:0,sent:{size},hit:{size}\n".format(size=str(len(vlist)-1))
			for v in vlist:
				rtn += "{}\n".format(','.join(v))

		return rtn


	def query(self,q):

		rtnobj = ng.run(self.golapOBJ,q)		

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

		#return ng.run0(self.golapOBJ,q)




	def save(self):
		return ng.save(self.golapOBJ)
	
	def getConf(self):
		try:
			return { "sts":0 , "rtn" : json.dumps(self.jsondf,ensure_ascii=False,indent=4) }
		except:
			return { "sts":-1, "rtn" : "Failed to convert json" }
		
