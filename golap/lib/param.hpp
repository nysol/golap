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

#ifndef param_hpp
#define param_hpp

#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional.hpp>
#include <boost/interprocess/sync/file_lock.hpp>

using namespace std;

namespace kgmod {
    class Param {
        string ParamFile;
        string originalText;
        boost::interprocess::file_lock* flock;
        boost::property_tree::ptree pt;
        
    public:
        Param(string& ParamFile);
        ~Param(void);
        
        bool WriteParam(void);
        bool ReadParam(void);
        bool convJson(string& json);
        bool SetFilePath(const string File, const string& FilePath);
        bool GetFilePath(const string File, string& FilePath);
        template<typename T> boost::optional<T> get(const string& path) {
            return pt.get_optional<T>(path);
        }

    private:
        void FileLock(void);
        void FileUnlock(void);
    };
}

#endif /* param_hpp */
