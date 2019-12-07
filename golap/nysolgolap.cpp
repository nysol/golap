// helloWrapper.c
#include "Python.h"
//#include <kgEnv.h>
//#include <kgMethod.h>
//#include <kgCSV.h>
#include <kggolap.hpp>
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

//static char* strGET(PyObject* data){
//static bool strCHECK(PyObject* data){
#if PY_MAJOR_VERSION >= 3
 #define strGET   PyUnicode_AsUTF8
 #define strCHECK PyUnicode_Check
#else		
 #define strGET   PyString_AsString
 #define strCHECK PyString_Check
#endif


/*
PyObject* pivot(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}

PyObject* worksheet(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}



PyObject* nodeimage(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}


PyObject* nodestat(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}


PyObject* query(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}


PyObject* retrieve(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}



PyObject* control(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		PyObject *req;
		if (!PyArg_ParseTuple(args, "OO", &ol , &req )){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		return PyLong_FromLong(1);

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}
*/

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

PyObject* run(PyObject* self, PyObject* args)
{
	try {
		PyObject *ol;
		char* data;
		if (!PyArg_ParseTuple(args, "Os", &ol, &data)){
 	   return Py_BuildValue("");
		}
		kgGolap *kolap	= (kgGolap *)PyCapsule_GetPointer(ol,"kggolapP");
		string str = kolap->proc(data);
		return PyUnicode_FromString(str.c_str());

	}catch(...){
		std::cerr << "run Error [ unKnown ERROR ]" << std::endl;
	}
	return PyLong_FromLong(1);
}



PyObject* start(PyObject* self, PyObject* args){

	char *fname;
	if (!PyArg_ParseTuple(args, "s", &fname)){
    return Py_BuildValue("");
  }
	return PyCapsule_New(new kgGolap(fname),"kggolapP",py_kggolap_free);

}


static PyMethodDef callmethods[] = {
	{"init", 	   reinterpret_cast<PyCFunction>(start)    , METH_VARARGS },
	{"load", 	   reinterpret_cast<PyCFunction>(load)     , METH_VARARGS },
	{"run", 	   reinterpret_cast<PyCFunction>(run)     , METH_VARARGS },
//	{"control",  reinterpret_cast<PyCFunction>(control)  , METH_VARARGS },
//	{"retrieve", reinterpret_cast<PyCFunction>(retrieve) , METH_VARARGS },
//	{"query",	   reinterpret_cast<PyCFunction>(query)    , METH_VARARGS },
//	{"nodestat", reinterpret_cast<PyCFunction>(nodestat) , METH_VARARGS },
//	{"nodeimage",reinterpret_cast<PyCFunction>(nodeimage), METH_VARARGS },
//	{"worksheet",reinterpret_cast<PyCFunction>(worksheet), METH_VARARGS },
//	{"pivot",    reinterpret_cast<PyCFunction>(pivot)    , METH_VARARGS },
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
