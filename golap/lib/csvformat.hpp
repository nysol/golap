#pragma once
#include <string>

namespace kgmod {

	class CsvFormat{
		size_t _fldCnt;
		vector<string> _head;
		vector< vector<string> > _body;

		public:
		CsvFormat():_fldCnt(0){}

		CsvFormat(size_t size){
			_fldCnt = size;
			_head.resize(size);
			_body.resize(size);
		}
		void pushData(int pos ,string val ){
			_body[pos].push_back(val);
		}
		void setHeader(int pos , string val ){
			_head[pos] = val; 
		}
		void setHeader(vector<string>& hed ){
			for (size_t i=0 ; i<hed.size() ;i++ ){
				if(i >= _fldCnt){ break;}
				_head[i] = hed[i]; 
			}
		}

		string getData(size_t pos,size_t line ){
			return _body[pos][line];
		}


		size_t fldSize(){ return _fldCnt;}
		size_t lineSize(){ return _body[0].size();}


	};
}