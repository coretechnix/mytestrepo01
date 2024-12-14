#pragma once
#include <condition_variable>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <atomic>
#include <vector>
#include <deque>
#include <unordered_map>
#include <map>
#include <chrono>
#ifdef OS_WIN
        #undef UNICODE
        #define WIN32_LEAN_AND_MEAN
        #include <windows.h>
        #include <winsock2.h>
        #include <ws2tcpip.h>
        #include <stdlib.h>
        #include <stdio.h>
        // Need to link with Ws2_32.lib
        #pragma comment (lib, "Ws2_32.lib")
        // #pragma comment (lib, "Mswsock.lib")
#else
        /* Assume that any non-Windows platform uses POSIX-style sockets instead. */
        #include <sys/types.h>
        #include <netinet/in.h>
        
        #include <sys/un.h>
        #include <sys/types.h>
        
        #include <netinet/tcp.h>
        #include <sys/socket.h>
        #include <arpa/inet.h>
        #include <sys/select.h>
        #include <sys/time.h>
        #include <netdb.h>  /* Needed for getaddrinfo() and freeaddrinfo() */
        #include <unistd.h> /* Needed for close() */
        #include <errno.h>
#endif

#ifndef OS_WIN
        #ifndef INVALID_SOCKET
        #define INVALID_SOCKET -1 // on Windows: #define INVALID_SOCKET  (SOCKET)(~0)
        #endif
        #ifndef SOCKET_ERROR
        #define SOCKET_ERROR -1
        #endif
#endif

#ifndef  OS_WIN
typedef int SOCKET;
#endif

namespace spdlog
{
   class logger;
};

namespace zcomm {
    class Publisher;
    class Subscriber;
};

namespace System {
    namespace Config {
        namespace Event {
            namespace Distr {
                namespace Server {
                    class Config;
                    class ModuleCommServer;
                    class PublisherServer;
                    class PublisherServers;
                }
            }
        }
    }

    namespace Comm {
        class Message;
    }
};

typedef System::Config::Event::Distr::Server::Config config_t;
typedef System::Config::Event::Distr::Server::ModuleCommServer mod_comm_svr_t;
typedef System::Config::Event::Distr::Server::PublisherServer  pub_svr_t;
typedef System::Config::Event::Distr::Server::PublisherServers pub_svrs_t;

typedef System::Comm::Message system_msg_t;

template <class T>
class ths_deque
{
    private:
            typedef std::deque<T> deque;
            deque data;
            mutable std::shared_mutex mtx;
    public:
        ths_deque()
        {}
        ths_deque(ths_deque&&) = default;
        ~ths_deque()
        {
            if ( !empty() ) clear();
        }
        void push(const T &_value)
        {
            std::lock_guard<std::shared_mutex> _(mtx);
            data.push_back( _value );
        }
        const T& front() const
        {
            std::shared_lock<std::shared_mutex> _(mtx);
            return data.front();
        }
        void pop()
        {
            std::lock_guard<std::shared_mutex> _(mtx);
            if ( !data.empty() )
            {
                data.pop_front();
            }
        }
        bool empty() const
        {
            std::shared_lock<std::shared_mutex> _(mtx);
            return data.empty();
        }
        size_t size() const
        {
            std::shared_lock<std::shared_mutex> _(mtx);
            return data.size();
        }
        void clear()
        {
            std::lock_guard<std::shared_mutex> _(mtx);
            data.clear();
        }
};
template<typename Data>
class concurrent_queue
{
private:
    std::deque<Data> the_queue;
    mutable std::mutex the_mutex;
    //std::condition_variable the_condition_variable;
public:
    void push(Data const& data)
    {
        std::scoped_lock lock(the_mutex);
        //the_queue.push(data);
        the_queue.push_back(data);
        //the_condition_variable.notify_one();
    }

    bool empty() const
    {
        std::scoped_lock lock(the_mutex);
        return the_queue.empty();
    }

    int size() const
    {
        std::scoped_lock lock(the_mutex);
        return the_queue.size();
    }

    void clear()
    {
        std::scoped_lock lock(the_mutex);
        the_queue.clear();
        //the_condition_variable.notify_one();
    }

    bool try_pop(Data& popped_value)
    {
        std::scoped_lock lock(the_mutex);
        if (the_queue.empty())
        {
            return false;
        }

        popped_value=the_queue.front();
        the_queue.pop_front();
        return true;
    }
    bool front(Data& front_value)
    {
        std::scoped_lock lock(the_mutex);
        if (the_queue.empty()) return false;
        front_value=the_queue.front();
        return true;
    }
    bool back(Data& back_value)
    {
        std::scoped_lock lock(the_mutex);
        if (the_queue.empty()) return false;
        back_value=the_queue.back();
        return true;
    }
    bool pop()
    {
        std::scoped_lock lock(the_mutex);
        if (the_queue.empty()) return false;
        the_queue.pop_front();
        return true;
    }
    std::deque<Data> get_queue() const
    {
        std::scoped_lock lock(the_mutex);
        return the_queue;
    }
    /*void wait_and_pop(Data& popped_value)
    {
        std::scoped_lock lock(the_mutex);
        while(the_queue.empty())
        {
            the_condition_variable.wait(lock);
        }

        popped_value=the_queue.front();
        the_queue.pop();
    }*/

};
class ChannelEventDistributor
{
    private:
    

            mutable std::shared_mutex mtx_config;
            std::shared_ptr<config_t> configPtr_;
            std::shared_ptr<config_t>& get_config();
            const std::shared_ptr<config_t>& get_config() const;
            bool set_config(const std::string &_json_str);
            std::mutex mtx_cfg_fn_update_;
            bool update_config_file();
            
            std::string config_file_path_;
            void set_config_file_path(const std::string &_value);
            std::string config_file_path() const;
            
            std::string logger_uuid_;
            void set_logger_uuid(const std::string &_value);
            std::string logger_uuid() const;
            std::shared_ptr<spdlog::logger> log;
            void create_logger();

            std::atomic<bool> running_;

            struct ch_dist_res_t {
                //std::string chid_;
                ths_deque<system_msg_t> q_;
                std::thread th_;
                std::mutex mtx_;
                std::condition_variable cv_;
                int bind_port_=0;
                //int port_offset_=0;
                std::chrono::seconds ttl_=std::chrono::seconds::zero();
            };
            
            // distribution flow per channel id
            typedef std::string chid_t;
            typedef std::unordered_map<chid_t,ch_dist_res_t> sch_dist_q_t;
            sch_dist_q_t sch_dist_queues_;
            bool init_sch_dist_queues();
            int add_new_pub_svr_to_config(const std::string &_chid,pub_svr_t **_svr);
            int remove_pub_svr_from_config(const std::string &_chid);
            //int get_distr_server_bind_port(const pub_svr_t *_svr,const int &_idx);
            int get_distr_server_real_bind_port(pub_svr_t *_svr,const int &_idx,const bool &_dalloc=false);
            bool get_config_distr_server_free_bind_port(int &_bind_port);
            bool calculate_real_distr_server_bind_port(int &_bind_port);
            //static bool compare_ports_min(std::pair<chid_t,ch_dist_res_t> i, std::pair<chid_t,ch_dist_res_t> j);
            //static bool compare_ports_max(std::pair<chid_t,ch_dist_res_t> i, std::pair<chid_t,ch_dist_res_t> j);
            //int get_lowest_port(const sch_dist_q_t &_mymap);
            //int get_highest_port(const sch_dist_q_t &_mymap);
            int get_lowest_port();
            int get_highest_port();
            
            std::mutex mtx_remove_dq_;
            bool remove_sch_dist_queue(const std::string &_chid);
            std::vector<std::thread> threads_;
            std::mutex mtx_threads_;
            std::vector<std::thread>& get_threads();
            
            std::mutex mtx_cv_publisher_;
            std::condition_variable cv_publisher_;
            void notify_publisher_thread();
            
            //std::mutex mtx_cv_notify_publisher_;
            //std::condition_variable  cv_notify_publisher_;
            void internal_notify_publisher_thread();
            void internal_notify_subcriber_thread();
            std::atomic<bool> notified_=false;
            std::mutex mtx_notify_;
            std::condition_variable cv_notify_;
            void notify();
            
            
            // ### create mutex protected functions for these...
            SOCKET* tcp_portsvc_server_socket_;
            SOCKET* tcp_modcomm_server_socket_;
            SOCKET* ipc_modcomm_server_socket_;
            SOCKET* tcp_check_server_socket_;

            // threads
            void tcp_check_server_thread();
            void TcpCheckClientConnectionHandler(SOCKET _ClientSocket, const std::string &_host);

            // threads
            void module_tcp_comm_server_thread();
            void module_ipc_comm_server_thread();
            void ClientConnectionHandler(SOCKET _ClientSocket, const std::string &_host);
            
            void publisher_portsvc_server_thread();
            void PortSvcClientConnectionHandler(SOCKET _ClientSocket, const std::string &_host);
            
            void channel_event_publisher_thread();
            //void channel_event_publisher_server_thread(pub_svr_t *_svr, const int _port_offset);
            void channel_event_publisher_server_thread(pub_svr_t *_svr);

            bool add_new_distr_queue(const std::string &_chid);
            
            //ths_deque<int> q_reusable_ports_;
            concurrent_queue<int> ccq_reusable_ports_;
            
            ths_deque<system_msg_t> chsch_msg_list_;
            
            // ### check this - not works properly !!!
            std::unique_ptr<zcomm::Publisher> event_publisher;

            std::string timestamp();
            
            bool read_file(const std::string &_file_path, std::string &_data, std::string &_errm, int &_errc);
            bool save_file(const std::string &_file_path, const std::string &_data, std::string &_errm, int &_errc);

            bool serialize_pbmsg_to_codedstream(const system_msg_t &_msg, char **_ackBuf, int &_bufsize);
            bool parse_pbmsg_from_codedstream(system_msg_t &_msg, const std::string &_data);
            
            mutable std::mutex mtx_portlist_;
            std::map<std::string,std::string> chid_distr_server_portlist_;
            void update_chid_distr_server_portlist();
            std::map<std::string,std::string> get_chid_distr_server_portlist() const;
            //bool create_channel_event_psvc_msg(system_msg_t &_msg);
            bool create_port_svc_msg(system_msg_t &_msg);
            
            /*typedef std::pair<ChannelEventDistributor::chid_t,ChannelEventDistributor::ch_dist_res_t> MyPairType;
            struct CompareSecond
            {
                bool operator()(const MyPairType& left, const MyPairType& right) const
                {
                    return left.second.bind_port_ < right.second.bind_port_;
                }
            };*/
            
            // zmq context
            void* zmq_ctx;//=zmq_ctx_new();
            const std::string inproc_bind_address="inproc://#notify";
    protected:
    public:
            ChannelEventDistributor();
            ~ChannelEventDistributor();
            bool read_config_file(const std::string& _config_file_path);
            bool init();
            void run();
            bool terminate();
};
