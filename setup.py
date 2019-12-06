from setuptools import setup, find_packages, Extension
import re
import os
import subprocess

#args = ['xml2-config','--cflags']
#xmlcflg= subprocess.check_output(args).decode().rstrip().split()
#args = ['xml2-config','--libs']
#xmllibs= subprocess.check_output(args).decode().rstrip().split()

#hedears = ['src','src/kgmod','src/mod']

def checkLibRun(cc,fname,paras):
	for para in paras:
		try:
			with open(os.devnull, 'w') as fnull:
				exit_code = subprocess.call(cc + [ fname , "-l"+para],
                                    stdout=fnull, stderr=fnull)
		except OSError :
			exit_code = 1

		if exit_code == 0:
			return para

	return ""


def check_for_boost():

	import sysconfig
	import tempfile
	import shutil
	
	# Create a temporary directory
	tmpdir = tempfile.mkdtemp()
	curdir = os.getcwd()
	os.chdir(tmpdir)

	compiler = os.environ.get('CC', sysconfig.get_config_var('CC'))

	# make sure to use just the compiler name without flags
	compiler = compiler.split()
	filename = 'test.cpp'
	with open(filename,'w') as f :
		f.write("""
			int main ()
			{
				return main ();
			  return 0;
			}
			""")

	boostFLG =[]
	boostLibLists=[
		['boost_thread-mt','boost_thread'],
		['boost_filesystem-mt','boost_filesystem'],
		['boost_regex-mt','boost_regex'],
		['boost_system-mt','boost_system'],
		['boost_date_time-mt','boost_date_time']
	]
	for boostlist in boostLibLists:

		boostflg = checkLibRun(compiler,filename, boostlist)
		if boostflg =="" : 	
			#err
			raise Exception("not find "+ " or ".join(boostlist))

		boostFLG.append(boostflg)

	# Clean up
	os.chdir(curdir)
	shutil.rmtree(tmpdir)

	return boostFLG


nmodLibs = ['pthread','kgmod3']
nmodLibs.extend(check_for_boost())


golapmod = Extension('nysolgolap/_nysolgolap_core',
	sources = ['golap/nysolgolap.cpp','golap/mod/kggolap.cpp',
'golap/lib/btree.cpp','golap/lib/cmdcache.cpp','golap/lib/cmn.cpp',
'golap/lib/config.cpp','golap/lib/facttable.cpp','golap/lib/filter.cpp',
'golap/lib/itematt.cpp','golap/lib/occ.cpp','golap/lib/param.cpp',
'golap/lib/request.cpp','golap/lib/traatt.cpp'
	],
	include_dirs=['golap','golap/lib','golap/mod'],
	libraries=nmodLibs,
	extra_compile_args=['-std=gnu++11', '-fpermissive']
)

setup(name = 'nysolgolap',
	packages=['nysolgolap'],
	version = '0.0.1',
	description = 'This is nysol tools',
	long_description="""
golap
""",
	author='nysol',
	author_email='info@nysol.jp',
	license='AGPL3',
	url='http://www.nysol.jp/',
  classifiers=[ 
		'Development Status :: 4 - Beta',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: GNU Affero General Public License v3',
		'Operating System :: POSIX :: Linux',
		'Operating System :: MacOS :: MacOS X',
		'Programming Language :: C',
		'Programming Language :: C++',
		'Programming Language :: Python :: 3',
		'Topic :: Software Development :: Libraries :: Python Modules',
		'Topic :: Scientific/Engineering :: Information Analysis',
		'Topic :: Scientific/Engineering :: Mathematics',
	],
	scripts=['scripts/golapsrv.py'],
	ext_modules =[golapmod]
)
       
