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
#include <numeric>
#include <float.h>
#include <functional>
#include <time.h>
#include <boost/optional.hpp>
#include "bidx-ewah.hpp"

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
            string Make(const vector<string> Items, const string delim = ",");
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
        bool StartWith(const string& str, const string& pref);
        vector<string> Split(const string &s, char delim);
        inline void ToUpper(string& s) {transform(s.cbegin(), s.cend(), s.begin(), ::toupper);}
        double DiffTime(const timespec& Start, const timespec End);
        static inline void chomp(string &str) { // 後ろの余白（スペースや改行）を削除する
            str.erase(find_if(str.rbegin(), str.rend(), [](int ch) {
                return (! isspace(ch));
            }).base(), str.end());
        }
        bool isDigit(char* str);
        bool ReplaceString(string& str, string fm, string to);
        static inline void EraseLastChar(string& str) {if (! str.empty()) str.erase(--str.end());}
        void readJson(string file);
        
        float calcPmi(size_t freq, size_t freq1, size_t freq2, size_t total);
        void timeStamp(string& datetime);
//        template <class T> boost::optional<size_t> posInVector(vector<T> vec, T target);
        boost::optional<size_t> posInVector(const vector<string>& vec, const string& target);
        
        // Min
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U min(Container<U>& x) {return *min_element(x.begin(), x.end());}
        // Max
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U max(Container<U>& x) {return *max_element(x.begin(), x.end());}
        // Sum
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U sum(Container<U>& x) {return accumulate(x.begin(), x.end(), 0.0);}
        // Mean
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U mean(Container<U>& x) {return accumulate(x.begin(), x.end(), 0.0) / x.size();}
        // Median
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U median(Container<U> x) {
            size_t n = x.size() / 2;
            nth_element(x.begin(), x.begin() + n, x.end());
            if (x.size() % 2 == 1) {
                return x[n];
            } else {
                return (x[n - 1] + x[n]) / 2;
            }
        }
        // Var
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U var(Container<U>& x) {
            size_t n = x.size();
            if (n < 2) return -DBL_MAX;
            U x_mean = mean(x);
            return (inner_product(x.begin(), x.end(), x.begin(), 0.0) - (pow(x_mean, 2) * n)) / (n - 1.0);
        }
        // SD
        template <typename U, template<class T, class Allocator = allocator<T>> class Container>
        U sd(Container<U>& x) {
            if (x.size() < 2) return -DBL_MAX;
            return sqrt(var(x));
        }
    }
}
#endif /* cmn_hpp */
