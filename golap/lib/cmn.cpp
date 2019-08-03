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

#include <iostream>
#include <sstream>
#include <cmath>
#include <time.h>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "bidx-ewah.hpp"
//#include "addrtable.hpp"
//#include "csvfile.hpp"
#include "cmn.hpp"

using namespace std;
using namespace kgmod;

char* kgmod::Cmn::CsvStr::SplitValues(char *p, char *field, int size) {
    if (*p == '"') {
        // ここでのインクリメントは
        // 最後のインクリメントではポインタが＋２になる(評価された場合のみ)
        // *p != '"'が0になった場合のみ *(++p) == '"'が実行される
        // これはダブルクォート連続の時の対策
        while (*(++p) && *p != '\n' && (*p != '"' || *(++p) == '"')) {
            if (--size > 0) {
                *(field++) = *p;
            }
        }
    }
    // ここの部分は""で囲まれていない部分が通るようになっている
    for ( ; *p && *p != ',' && *p != '\n'; p++) {
        if (--size > 0) {
            *(field++) = *p;
        }
    }
    *field = '\0';
    return *p ? (p + 1) : p;
}

vector<string> kgmod::Cmn::CsvStr::Parse(const string Record) {
    vector<string> Fields;
    if (Record == "") {
        Fields.clear();
        return Fields;
    }
    char* p = (char*)Record.c_str();
    char* field;
    const size_t allocsize = 10240;
    field = (char*)malloc(allocsize);
    if (field == NULL) {throw new Cmn::Exception("malloc error in Csv::Parse");}
    memset(field, '\0', allocsize);
    while (*p) {
        p = CsvStr::SplitValues(p, field, allocsize);
        Fields.push_back(field);
        memset(field, '\0', strlen(field));
    }
    if (Record.substr(Record.size()-1) == ",") {
        Fields.push_back("");
    }
    free(field);
    return Fields;
}
string kgmod::Cmn::EnvFile(const char* PrefEnv, const string Filename, const char* ext) {
    string Pref(getenv(PrefEnv));
    if (ext == NULL) {
        return Pref + Filename;
    } else {
        boost::filesystem::path fn(Filename);
        return Pref + fn.stem().string() + string(ext);
    }
}

string kgmod::Cmn::FullPath(const string Path) {
    boost::filesystem::path RelPath(Path);
    return boost::filesystem::system_complete(RelPath).string();
}

string kgmod::Cmn::AbsPath(const string Path, const string Base) {
    boost::filesystem::path RelPath(Path);
    return boost::filesystem::absolute(RelPath, Base).string();
}

string kgmod::Cmn::ParentPath(const string Path) {
    boost::filesystem::path RelPath(Path);
    boost::filesystem::path AbsPath = boost::filesystem::absolute(RelPath);
    return AbsPath.parent_path().string();
}

string kgmod::Cmn::Stem(const string Filename) {
    boost::filesystem::path fn(Filename);
    return fn.stem().string();
}

string kgmod::Cmn::ReplaceExtension(const string Filename, const string Extension) {
    boost::filesystem::path fn(Filename);
    return fn.replace_extension(Extension).string();
}

bool kgmod::Cmn::isDirectory(const string Path) {
    boost::filesystem::path Dir(Path);
    return boost::filesystem::is_directory(Dir);
}

bool kgmod::Cmn::FileExists(const string Path) {
    boost::filesystem::path fn(Path);
    return boost::filesystem::exists(fn);
}

size_t kgmod::Cmn::FileSize(const string Path) {
    boost::filesystem::path fn(Path);
    boost::uintmax_t size = boost::filesystem::file_size(fn);
    return (size_t)size;
}

bool kgmod::Cmn::MkDir(const string Path) {
    if (kgmod::Cmn::FileExists(Path)) return true;
    boost::system::error_code error;
    boost::filesystem::path pn(Path);
    bool stat = boost::filesystem::create_directories(pn, error);
    if (!stat || error) {
        cerr << "#ERROR# filed to create directory " << Path << endl;
    }
    return stat;
}

bool kgmod::Cmn::DelFile(const string Path) {
    if (! kgmod::Cmn::FileExists(Path)) return true;
    boost::system::error_code error;
    boost::filesystem::path fn(Path);
    bool stat =  boost::filesystem::remove(fn, error);
    if (!stat || error) {
        cerr << "#ERROR# filed to create directory " << Path << endl;
    }
    return stat;
}


size_t kgmod::Cmn::Find(const vector<string>List, string Value) {
    auto ite = find(List.begin(), List.end(), Value);
    if (ite == List.end()) {
        return -1;
    }
    return distance(List.begin(), ite);
}

void kgmod::Cmn::CheckEwah(Ewah& bmp) {
    cerr << "(" << bmp.numberOfOnes() << ")";
    const size_t max_print = 20;
    size_t c = 0;
    for (auto i = bmp.begin(); i != bmp.end(); i++) {
        cerr << " " << i.answer;
        c++;
        if (c > max_print) break;
    }
    if (c > max_print) cerr << "...";
    cerr << endl;
}

void kgmod::Cmn::CheckEwah(Ewah* bmp) {
    cerr << "(" << bmp->numberOfOnes() << ")";
    const size_t max_print = 20;
    size_t c = 0;
    for (auto i = bmp->begin(); i != bmp->end(); i++) {
        cerr << " " << i.answer;
        c++;
        if (c > max_print) break;
    }
    if (c > max_print) cerr << "...";
    cerr << endl;
}

bool kgmod::Cmn::MatchWild(const char* pat, const char* str) {
    switch(*pat) {
        case '\0':
            return (*str == '\0');
        case '*':
            return MatchWild(pat + 1, str) || ((*str != '\0') && MatchWild(pat, str + 1));
        case '?':
            return (*str != '\0') && MatchWild(pat + 1, str + 1);
        default:
            return (*pat == *str) && MatchWild(pat + 1, str + 1);
    }
}

bool kgmod::Cmn::StartWith(const string& str, const string& pref) {
    size_t size = pref.size();
    if (str.size() < size) return false;
    return equal(begin(pref), end(pref), std::begin(str));
}

vector<string> kgmod::Cmn::Split(const string& s, char delim) {
    vector<string> elems;
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim)) {
        if (!item.empty()) {
            elems.push_back(item);
        }
    }
    return elems;
}

double kgmod::Cmn::DiffTime(const timespec& Start, const timespec End) {
    const size_t nsec = 1000000000;     // 1,000,000,000
    double s = Start.tv_sec + (double)Start.tv_nsec / nsec;
    double e = End.tv_sec + (double)End.tv_nsec / nsec;
    return e - s;
}

bool kgmod::Cmn::isDigit(char* str) {
    bool res = true;
    for (size_t c = 0; c < strlen(str); c++) {
        res = (bool)isdigit(str[c]);
        if (! res) break;
    }
    return res;
}

bool kgmod::Cmn::ReplaceString(string& str, string fm, string to) {
    bool out = false;
    size_t pos(str.find(fm));
    while(pos != string::npos) {
        out = true;
        str.replace(pos, fm.length(), to);
        pos = str.find(fm, pos + to.length());
    }
    return out;
}

float kgmod::Cmn::calcPmi(size_t freq, size_t freq1, size_t freq2, size_t total) {
    float P_u_v = (float)freq / total;
    float P_u = (float)freq1 / total;
    float P_v = (float)freq2 / total;
    float x1 = log(P_u_v / (P_u * P_v));
    float x2 = (-1) * log(P_u_v);
    float pmi = x1 / x2;
    return pmi;
}

void kgmod::Cmn::timeStamp(string& datetime) {
    char buf[20];
    time_t timer = time(NULL);
    struct tm* tim = localtime(&timer);
    strftime(buf, sizeof(buf), "%Y%m%d%H%M%S", tim);
    datetime = buf;
}

//template <class T>
//boost::optional<size_t> kgmod::Cmn::posInVector(vector<T> vec, T target) {
boost::optional<size_t> kgmod::Cmn::posInVector(vector<string> vec, string target) {
    boost::optional<size_t> ret;
    auto it = find(vec.begin(), vec.end(), target);
    if (it == vec.end()) {
        return boost::none;
    } else {
        return distance(vec.begin(), it);
    }
}
