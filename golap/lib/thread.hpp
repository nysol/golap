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

#ifndef thread_hpp
#define thread_hpp

#include <queue>
#include <boost/thread/mutex.hpp>
//#include "thread.hpp"

namespace kgmod {
    template<typename T>
    class MtQueue {
    public:
        typedef pair<size_t, T> th_t;
        void push(th_t* th) {
            mtx.lock();
            q.push(th);
            mtx.unlock();
        }
        th_t* pop() {
            pair<size_t, T>* th;
            mtx.lock();
            if (q.empty()) {
                th = NULL;
            } else {
                th = q.front();
                q.pop();
            }
            mtx.unlock();
            return th;
        }
        
    private:
        queue<pair<size_t, T>*> q;
        boost::mutex mtx;
    };
}

#endif /* thread_hpp */
