// helloWrapper.c
#include "Python.h"
//#include <kgEnv.h>
//#include <kgMethod.h>
//#include <kgCSV.h>
#include <kggolap.hpp>
#include <kgcregdb.hpp>
//using namespace kgmod;
//using namespace kglib;
#include <iostream>
#if PY_MAJOR_VERSION >= 3
extern "C" {
	PyMODINIT_FUNC PyInit__nysolshell_core(void);
}
#else
extern "C" {
	void init_nysolshell_core(void);
}
#endif

#if PY_MAJOR_VERSION >= 3
 #define strGET   PyUnicode_AsUTF8
 #define strCHECK PyUnicode_Check
#else		
 #define strGET   PyString_AsString
 #define strCHECK PyString_Check
#endif


void py_kggolap_free(PyObject *obj){
	kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(obj,"kggolapP");
	if(kolap){ delete kolap;}
}

PyObject* load(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		if (!PyArg_ParseTuple(args, "O", &ol )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		kolap->prerun();
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}

PyObject* save(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		if (!PyArg_ParseTuple(args, "O", &ol )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		kolap->save();
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}


PyObject* getItmAtt(PyObject* self, PyObject* args)
{
	try {

		PyObject *ol;
		char* fldname;
		char* ifil = NULL;
		
		if (!PyArg_ParseTuple(args, "Os|s", &ol , &fldname ,&ifil  )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		vector<string> rtn;
		if( ifil == NULL){
			rtn = kolap->getItmAtt(fldname);
		}
		else{
			rtn = kolap->getItmAtt(fldname,ifil);		
		}

		PyObject *rtnList  = PyList_New(rtn.size());
		for(size_t i=0;i < rtn.size();i++){
			PyList_SetItem(rtnList,i,PyUnicode_FromString(rtn[i].c_str()));
		}

		return rtnList;

	}catch(kgError& err){
		std::cerr << err.message(0) << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(err.message(0).c_str());
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(char const * msg){
		std::cerr << msg << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(msg);
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(...){
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString("run Error [ unKnown ERROR ]");
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
}

PyObject* getTraAtt(PyObject* self, PyObject* args)
{
	try {

		PyObject *ol;
		char* fldname;
		if (!PyArg_ParseTuple(args, "Os", &ol , &fldname )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		vector<string> rtn = kolap->getTraAtt(fldname);

		PyObject *rtnList  = PyList_New(rtn.size());
		for(size_t i=0;i < rtn.size();i++){
			PyList_SetItem(rtnList,i,PyUnicode_FromString(rtn[i].c_str()));
		}

		return rtnList;

	}catch(kgError& err){
		std::cerr << err.message(0) << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(err.message(0).c_str());
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(char const *msg){
		std::cerr << msg << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(msg);
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(...){
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString("run Error [ unKnown ERROR ]");
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
}

PyObject* getNodeIMG(PyObject* self, PyObject* args)
{
	try {

		PyObject *ol;
		PyObject *jsonDict;
		if (!PyArg_ParseTuple(args, "OO", &ol , &jsonDict )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		//NodeImage nodeImageParams;
		string traFilter;
		string itemFilter;
		string gTransaction;
		string gnode;
		string itemVal;

		PyObject * ni = PyDict_GetItemString(jsonDict,"nodeimage");
		if(!ni){
			std::cerr << "parameter err" << std::endl;
		}
		PyObject * v;

		v = PyDict_GetItemString(ni,"traFilter");
		if(v){ traFilter = strGET(v); }

		v = PyDict_GetItemString(ni,"itemFilter");
		if(v){ itemFilter = strGET(v); }

		v = PyDict_GetItemString(ni,"itemVal");
		if(v){ itemVal = strGET(v); }
		else { cerr << "nodeimage.itemVal must be set in request" << endl;}


		PyObject *gv = PyDict_GetItemString(ni,"granularity");
		if(gv){
			PyObject * gvv;
			gvv = PyDict_GetItemString(gv,"transaction");
			if(gvv){
				gTransaction = strGET(gvv);
			}
			gvv = PyDict_GetItemString(gv,"gnode");
			if(gvv){
				gnode = strGET(gvv);
			}
		}
		CsvFormat rtn = kolap->nodeimage( traFilter,itemFilter,gTransaction,gnode,itemVal);


		PyObject *rtnList  = PyList_New(rtn.lineSize());
		for(size_t i=0;i < rtn.lineSize();i++){
			PyList_SetItem(rtnList,i,PyUnicode_FromString( rtn.getData(0,i).c_str() ) );
		}

		return rtnList;


	}catch(kgError& err){
		std::cerr << err.message(0) << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(err.message(0).c_str());
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(char const *msg){
		std::cerr << msg << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(msg);
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(...){
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString("run Error [ unKnown ERROR ]");
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
}


PyObject* getNodeStat(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *jsonDict;
		if (!PyArg_ParseTuple(args, "OO", &ol , &jsonDict )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		//NodeImage nodeImageParams;
		string traFilter;
		string itemFilter;
		string gTransaction;
		string gNode;
		string itemVal;
		string values;

		PyObject * ni = PyDict_GetItemString(jsonDict,"nodestat");
		if(!ni){
			std::cerr << "parameter err" << std::endl;
		}

		PyObject * v;

		v = PyDict_GetItemString(ni,"traFilter");
		if(v){ traFilter = strGET(v); }

		v = PyDict_GetItemString(ni,"itemFilter");
		if(v){ itemFilter = strGET(v); }

		v = PyDict_GetItemString(ni,"itemVal");
		if(v){
			if(strCHECK(v)){ itemVal = strGET(v); }
			else if(PyLong_Check(v)){
				long lv = PyLong_AsLong(v);
				itemVal = toString(lv);
			} 
			else{
				throw("nodestat.itemVal unsupprt datatype");
			}
		}
		else { 
			throw("nodestat.itemVal must be set in request");
		}

		v = PyDict_GetItemString(ni,"values");
		if(v){ values = strGET(v); }
		else { throw("nodestat.values must be set in request"); }

		PyObject *gv = PyDict_GetItemString(ni,"granularity");
		if(gv){
			PyObject * gvv;
			gvv = PyDict_GetItemString(gv,"transaction");
			if(gvv){
				gTransaction = strGET(gvv);
			}
			gvv = PyDict_GetItemString(gv,"node");
			if(gvv){
				gNode = strGET(gvv);
			}
		}
		vector< vector<string> > rtn = kolap->nodestat(
			traFilter,itemFilter,
			gTransaction,gNode,
			itemVal,values
		);
		PyObject *rtnList  = PyList_New(rtn.size());
		for(size_t i=0;i < rtn.size();i++){

			PyObject *rtnBList  = PyList_New(rtn[i].size());

			for(size_t j=0 ; j<rtn[i].size(); j++){
				PyList_SetItem(rtnBList,j,PyUnicode_FromString(rtn[i][j].c_str()));
			}

			PyList_SetItem(rtnList,i,rtnBList);
		}

		return rtnList;

	}catch(kgError& err){
		std::cerr << err.message(0) << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(err.message(0).c_str());
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(char const *msg){
		std::cerr << msg << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(msg);
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
	catch(...){
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString("run Error [ unKnown ERROR ]");
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
}

PyObject* run(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *jsonDict;
		if (!PyArg_ParseTuple(args, "OO", &ol, &jsonDict)){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");

		
		PyObject * ni = PyDict_GetItemString(jsonDict,"query");
		if(!ni){
			std::cerr << "parameter err" << std::endl;
		}
		string traFilter;
		string itemFilter;
		string gTransaction;
		string gNode;

		string SelMinSup;
		string SelMinConf;
		string SelMinLift;
		string SelMinJac;
		string SelMinPMI;

		string sortKey;
		string sendMax;
		string dimension;
		string deadline;

		PyObject * v;
		v = PyDict_GetItemString(jsonDict,"deadlineTimer");
		if(v){
			if(strCHECK(v)){ deadline = strGET(v);	}
			else if(PyLong_Check(v)){deadline = toString(PyLong_AsLong(v));}
		}

		v = PyDict_GetItemString(ni,"traFilter");
		if(v){ traFilter = strGET(v); }

		v = PyDict_GetItemString(ni,"itemFilter");
		if(v){ itemFilter = strGET(v); }

		v = PyDict_GetItemString(ni,"selCond");
		if(v){
			PyObject * vv;
			vv = PyDict_GetItemString(v,"minSup");
			if(vv){
				if(strCHECK(vv)){ SelMinSup = strGET(vv);	}
				else if(PyLong_Check(vv)){ SelMinSup = toString(PyLong_AsLong(vv));}
				else if(PyFloat_Check(vv)){ SelMinSup = to_string(PyFloat_AsDouble(vv));}
			}

			vv = PyDict_GetItemString(v,"minConf");
			if(vv){
				if(strCHECK(vv)){ SelMinConf = strGET(vv);	}
				else if(PyLong_Check(vv)){ SelMinConf = toString(PyLong_AsLong(vv));}
				else if(PyFloat_Check(vv)){ SelMinConf = to_string(PyFloat_AsDouble(vv));}
			}

			vv = PyDict_GetItemString(v,"minLift");
			if(vv){
				if(strCHECK(vv)){ SelMinLift = strGET(vv);	}
				else if(PyLong_Check(vv)){ SelMinLift = toString(PyLong_AsLong(vv));}
				else if(PyFloat_Check(vv)){ SelMinConf = to_string(PyFloat_AsDouble(vv));}
			}

			vv = PyDict_GetItemString(v,"minJac");
			if(vv){
				if(strCHECK(vv)){ SelMinJac = strGET(vv);	}
				else if(PyLong_Check(vv)){ SelMinJac = toString(PyLong_AsLong(vv));}
				else if(PyFloat_Check(vv)){ SelMinConf = to_string(PyFloat_AsDouble(vv));}
			}

			vv = PyDict_GetItemString(v,"minPMI");
			if(vv){
				if(strCHECK(vv)){ SelMinPMI = strGET(vv);	}
				else if(PyLong_Check(vv)){ SelMinPMI = toString(PyLong_AsLong(vv));}
				else if(PyFloat_Check(vv)){ SelMinConf = to_string(PyFloat_AsDouble(vv));}
			}
		}
		else{
			std::cerr << "parameter err" << std::endl;		
		}

		v = PyDict_GetItemString(ni,"sortKey");
		if(v){ sortKey = strGET(v); 	}

		v = PyDict_GetItemString(ni,"sendMax");
		if(v){
			if(strCHECK(v)){
				 sendMax = strGET(v);	
			}
			else if(PyLong_Check(v)){
				 sendMax = toString(PyLong_AsLong(v));
			}
			else{}
		}
		v = PyDict_GetItemString(ni,"dimension");
		if(v){ dimension = strGET(v); 	}

		PyObject *gv = PyDict_GetItemString(ni,"granularity");
		if(gv){
			PyObject * gvv;
			gvv = PyDict_GetItemString(gv,"transaction");
			if(gvv){
				gTransaction = strGET(gvv);
			}
			gvv = PyDict_GetItemString(gv,"node");
			if(gvv){
				gNode = strGET(gvv);
			}
		}
		map<string, Result> vstr = kolap->runQuery(
			traFilter,itemFilter,gTransaction,gNode,
			SelMinSup,SelMinConf,SelMinLift,SelMinJac,SelMinPMI,
			sortKey,sendMax,dimension,deadline
		);

		if ( dimension.empty() ){

			PyObject* rlist = PyDict_New();
			
			PyObject* vv ;
			
			vv = PyLong_FromLong(vstr[""].status());
			PyDict_SetItemString(rlist,"status",vv);
			Py_DECREF(vv);

			vv = PyLong_FromLong(vstr[""].sentCnt());
			PyDict_SetItemString(rlist,"sent",vv);
			Py_DECREF(vv);

			vv = PyLong_FromLong(vstr[""].hitCnt());
			PyDict_SetItemString(rlist,"hit",vv);
			Py_DECREF(vv);
			
			
			PyObject* head = PyList_New(vstr[""].fldCnt());
			for(size_t i=0 ; i < vstr[""].fldCnt() ; i++){
				string hname = vstr[""].fldName(i);
				PyList_SetItem(head,i,PyUnicode_FromString(hname.c_str()));
			}
			PyDict_SetItemString(rlist,"header", head);
			Py_DECREF(head);


			vector< vector<string> > rtn = vstr[""].getdata();
			
			PyObject* rblist = PyList_New(rtn.size());

			for(size_t i=0 ;i<rtn.size();i++){

				PyObject* rbblist = PyList_New(rtn[i].size());

				for(size_t j=0 ;j<rtn[i].size();j++){
					const char* dname = rtn[i][j].c_str();
					PyList_SetItem(rbblist,j,PyUnicode_FromStringAndSize(dname ,strlen(dname)));
				}

				PyList_SetItem(rblist,i,rbblist);

			}
			PyDict_SetItemString(rlist,"data", rblist);
			Py_DECREF(rblist);

			return rlist;			

		}
		else{
			string rtn = "";

			vector<string> dstr = splitToken(dimension,',');

			PyObject* rblists = PyList_New(dstr.size()-1);

			for(size_t ai=1; ai<dstr.size();ai++){
				PyObject* rlist = PyDict_New();	
				PyObject* vv ;
			
				vv = PyLong_FromLong(vstr[dstr[ai]].status());
				PyDict_SetItemString(rlist,"status",vv);
				Py_DECREF(vv);

				vv = PyLong_FromLong(vstr[dstr[ai]].sentCnt());
				PyDict_SetItemString(rlist,"sent",vv);
				Py_DECREF(vv);

				vv = PyLong_FromLong(vstr[dstr[ai]].hitCnt());
				PyDict_SetItemString(rlist,"hit",vv);
				Py_DECREF(vv);

				vv = PyUnicode_FromString(dstr[0].c_str());
				PyDict_SetItemString(rlist,"dmName",vv);
				Py_DECREF(vv);

				vv = PyUnicode_FromString(dstr[ai].c_str());
				PyDict_SetItemString(rlist,"dmValue",vv);
				Py_DECREF(vv);


				PyObject* head = PyList_New(vstr[dstr[ai]].fldCnt());
				for(size_t i=0 ; i < vstr[dstr[ai]].fldCnt() ; i++){
					string hname = vstr[dstr[ai]].fldName(i);
					PyList_SetItem(head,i,PyUnicode_FromString(hname.c_str()));
				}
				PyDict_SetItemString(rlist,"header", head);
				Py_DECREF(head);


				vector< vector<string> > rtn = vstr[dstr[ai]].getdata();
			
				PyObject* rblist = PyList_New(rtn.size());

				for(size_t i=0 ;i<rtn.size();i++){
	
					PyObject* rbblist = PyList_New(rtn[i].size());

					for(size_t j=0 ;j<rtn[i].size();j++){
						const char* dname = rtn[i][j].c_str();
						PyList_SetItem(rbblist,j,PyUnicode_FromStringAndSize(dname ,strlen(dname)));
					}

					PyList_SetItem(rblist,i,rbblist);

				}
				PyDict_SetItemString(rlist,"data", rblist);
				Py_DECREF(rblist);
				
				PyList_SetItem(rblists,ai-1,rlist);
			}
		
			return rblists;

		}


	}catch(kgError& err){
		std::cerr << err.message(0) << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(err.message(0).c_str());
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
        
  }	catch(char const *msg){
		std::cerr << msg << std::endl;
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString(msg);
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}
  catch(...){
		PyObject* kerr = PyDict_New();	
		PyObject* kvv = PyLong_FromLong(-1);
		PyDict_SetItemString(kerr,"status",kvv);
		Py_DECREF(kvv);
		kvv = PyUnicode_FromString("run Error [ unKnown ERROR ]");
		PyDict_SetItemString(kerr,"errmsg",kvv);
		Py_DECREF(kvv);
		return kerr;
	}

}


PyObject* makeindex(PyObject* self, PyObject* args){

	char *fname;
	char *mpsize=NULL;

	if (!PyArg_ParseTuple(args, "s|s", &fname,&mpsize)){
    return Py_BuildValue("");
  }
  kgEnv    env;
  kgCreGdb kgmod;

	const char* vv[3];

	vv[0]="cregdb";

	// fname
	string ipara = "i="+string(fname);
	vv[1]=ipara.c_str();

	if(mpsize==NULL){
		kgmod.init(2, vv, &env);	
	}
	else{
		string mpara = "mp=" + string(mpsize);
		vv[2] = mpara.c_str();
		kgmod.init(3, vv, &env);
	}
	kgmod.run();

	return PyLong_FromLong(0); 

}



PyObject* start(PyObject* self, PyObject* args){

	char *fname;
	if (!PyArg_ParseTuple(args, "s", &fname)){
    return Py_BuildValue("");
  }
	return PyCapsule_New(new kgGolap(fname),"kggolapP",py_kggolap_free);

}


static PyMethodDef callmethods[] = {
	{"init"       ,reinterpret_cast<PyCFunction>(start)      , METH_VARARGS },
	{"makeindex"  ,reinterpret_cast<PyCFunction>(makeindex)  , METH_VARARGS },
	{"load" 	    ,reinterpret_cast<PyCFunction>(load)       , METH_VARARGS },
	{"save" 	    ,reinterpret_cast<PyCFunction>(save)       , METH_VARARGS },
	{"run" 	      ,reinterpret_cast<PyCFunction>(run)        , METH_VARARGS },
	{"getTraAtt"  ,reinterpret_cast<PyCFunction>(getTraAtt)  , METH_VARARGS },
	{"getItmAtt"  ,reinterpret_cast<PyCFunction>(getItmAtt)  , METH_VARARGS },
	{"getNodeIMG" ,reinterpret_cast<PyCFunction>(getNodeIMG) , METH_VARARGS },
	{"getNodeStat",reinterpret_cast<PyCFunction>(getNodeStat), METH_VARARGS },
	{NULL}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_nysolgolap_core",      /* m_name */
    NULL,							     /* m_doc */
    -1,                  /* m_size */
    callmethods,      /* m_methods */
    NULL,                /* m_reload */
    NULL,                /* m_traverse */
    NULL,                /* m_clear */
    NULL,                /* m_free */
};

PyMODINIT_FUNC
PyInit__nysolgolap_core(void){
	PyObject* m;
	m = PyModule_Create(&moduledef);
	return m;
}

#else

void init_nysolgolap_core(void){
	Py_InitModule("_nysolgolap_core", callmethods);
}

#endif
