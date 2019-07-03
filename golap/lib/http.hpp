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

#ifndef http_hpp
#define http_hpp

#include <boost/asio.hpp>
#include <boost/bind.hpp>

namespace asio = boost::asio;
using asio::ip::tcp;
using namespace std;
using namespace kgmod;

class Http {
    asio::io_service& io_service_;
    tcp::acceptor acceptor_;
    tcp::socket socket_;
    asio::streambuf receive_buff_;
    string send_data_;
    map<string, string> req_header_;
//    asio::steady_timer timer_; // タイムアウト用のタイマー
//    bool isTimerCanceled_;
//    size_t waitTimeInSec;

public:
    Http(asio::io_service& io_service, size_t port)
    : io_service_(io_service),
    acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
    socket_(io_service) {}
//    timer_(io_service), isTimerCanceled_(false), waitTimeInSec(10) {}
    
    void start(void) {start_accept();}
    void stop(void) {acceptor_.close();}
    void get_receive_buff(stringstream& ss) {ss << boost::asio::buffer_cast<const char*>(receive_buff_.data());}
    void put_send_data(string& body) {
        set_header(body.size());
        send_data_ += body;
    }
    void clear_send_data(void) {send_data_.clear();}
    void reserve_send_data(size_t size) {
        if (send_data_.capacity() < size) send_data_.reserve(size);
    }

private:
    void parse_req_header(void) {
        stringstream ss;
        ss << boost::asio::buffer_cast<const char*>(receive_buff_.data());
        string line;
        while (getline(ss, line)) {
            if (line.size() == 1) break;
            if (Cmn::StartWith(line, "POST")) req_header_["method"] = line + "\n";
            else if (Cmn::StartWith(line, "Host:")) req_header_["host"] = line + "\n";
        }
    }

    void set_header(size_t body_size) {
        send_data_  = req_header_["method"];
        send_data_ += req_header_["host"];
        send_data_ += "server: golap/0.1\r\n";
        send_data_ += "Content-Type: text/plain; charset=UTF-8\r\n";
        send_data_ += "Content-Length: " + to_string(body_size) + "\r\n";
        send_data_ += "\r\n";
    }
    
    void start_accept(void) {
        acceptor_.async_accept(socket_, boost::bind(&Http::on_accept, this, asio::placeholders::error));
        cerr << "ready" << endl;
    }
    
    void on_accept(const boost::system::error_code& error) {
        if (error) {
            cerr << "failed to accept: " << error.message() << std::endl;
        } else {
            cerr << "accepted" << endl;
            start_receive();
        }
    }
    
    void start_receive(void) {
        asio::async_read(socket_, receive_buff_, asio::transfer_at_least(1),
                         boost::bind(&Http::on_receive, this,
                                     asio::placeholders::error, asio::placeholders::bytes_transferred));
    }
    
    void on_receive(const boost::system::error_code& error, size_t bytes_transferred) {
        if (error && error != asio::error::eof) {
            cout << "failed to receive: " << error.message() << std::endl;
        } else {
//            timer_.expires_from_now(chrono::seconds(waitTimeInSec));
//            timer_.async_wait(boost::bind(&Http::on_timer, this, _1));
            parse_req_header();
            proc();
        }
    }
    
    void send(void) {
        asio::async_write(socket_, asio::buffer(send_data_), //asio::transfer_at_least(1),
                          boost::bind(&Http::on_send, this, asio::placeholders::error,
                                      asio::placeholders::bytes_transferred));
    }
    
    void on_send(const boost::system::error_code& error, size_t bytes_transferred) {
        if (error) {
            cout << "failed to send: " << error.message() << endl;
        } else {
//            cout << bytes_transferred << " bytes transfered" << endl;
        }
    }
    
/*****************************
    void on_timer(const boost::system::error_code& error) {
        if (!error && !isTimerCanceled_) {
//            socket_.cancel(); // 通信処理をキャンセルする。受信ハンドラがエラーになる
        }
    }
******************************/
    
protected:
    virtual void proc(void) {
        send();
        clear_send_data();
    };
};

#endif /* http_hpp */
