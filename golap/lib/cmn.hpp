/* ////////// LICENSE INFO ////////////////////
 
 * Copyright (C) 2013 by NYSOL CORPORATION
 *
 * Unless you have received this program directly from NYSOL pursuant
 * to the terms of a commercial license agreement with NYSOL, then
 * this program is licensed to you under the terms of the GNU Affero General
 * Public License (AGPL) as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY EXPRESS OR IMPLIED WARRANTY, INCLUDING THOSE OF
 * NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Please refer to the AGPL (http://www.gnu.org/licenses/agpl-3.0.txt)
 * for more details.
 
 ////////// LICENSE INFO ////////////////////*/

#ifndef cmn_hpp
#define cmn_hpp

#include <sstream>
#include <time.h>
#include "bidx-ewah.hpp"
//#include "csvfile.hpp"
//#include "storage.hpp"
//#include "param.hpp"

using namespace std;

namespace kgmod {
    namespace Cmn {
        class Exception : public exception {
        public:
            Exception(string msg) : msg(msg) {};
            string what(void) {return msg;}
        private:
            string msg;
        };
        namespace CsvStr {
            char* SplitValues(char *p, char *field, int size);
            vector<string> Parse(const string Record);
        }
        string EnvFile(const char* PrefEnv, const string Filename, const char* ext);
        string FullPath(const string Path);
        string AbsPath(const string Path, const string Base);
        string ParentPath(const string Path);
        string Stem(const string Filename);
        string ReplaceExtension(const string Filename, const string Extension);
        bool isDirectory(const string Path);
        bool FileExists(const string Path);
        size_t FileSize(const string Path);
        bool MkDir(const string Path);
        bool DelFile(const string Path);
        size_t Find(const vector<string>List, string value);
        void CheckEwah(Ewah& b);
        void CheckEwah(Ewah* b);
        bool MatchWild(const char* pat, const char* str);
        double DiffTime(const timespec& Start, const timespec End);
        static inline void chomp(string &str) { // 後ろの余白（スペースや改行）を削除する
            str.erase(find_if(str.rbegin(), str.rend(), not1(ptr_fun<int, int>(isspace))).base(), str.end());
        }
        bool isDigit(char* str);
        bool ReplaceString(string& str, string fm, string to);
        void readJson(string file);
        
        float calcPmi(size_t freq, size_t freq1, size_t freq2, size_t total);
    }
}
#endif /* cmn_hpp */
