import nysolgolap._nysolgolap_core as ng

class mgolap(object):

	def __init__(self,confF):
		self.golapOBJ = ng.init(confF)
		ng.load(self.golapOBJ)
		

	def query(self,q):
		return ng.run(self.golapOBJ,q)
