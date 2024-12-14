#pragma once
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <vector>
#include <iterator>
#include <utility>
#include <atomic>
#include <condition_variable>
#include <thread>

namespace spdlog
{
   class logger;
};

template <class T, class U>
class ths_unordered_map
{
    private:
            typedef std::unordered_map<T,U> unordered_map;
            unordered_map data;
            std::mutex mtx;
    public:
            ths_unordered_map()
            {}
            ~ths_unordered_map()
            {
                if ( 0<size() ) clear();
            }
            auto insert(const T &_key, const U &_value)
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.insert( std::make_pair<T,U>(_key,_value) );
            }
            template <class P>
            auto insert( P&& val )
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.insert( std::move (val) );
            }
            int count(const T &_value)
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.count(_value);
            }
            int size()
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.size();
            }
            void clear()
            {
                std::lock_guard<std::mutex> _(mtx);
                data.clear();
            }
            U& operator [] (T const &_key) {
                std::lock_guard<std::mutex> _(mtx);
                return data[_key];
            }
};

template <class T>
class ths_unordered_multiset
{
    private:
            typedef std::unordered_multiset<T> unordered_multiset;
            unordered_multiset data;
            std::mutex mtx;
    public:
            ths_unordered_multiset()
            {}
            ~ths_unordered_multiset()
            {
                if ( 0<size() ) clear();
            }
            auto insert(const T &_value)
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.insert(_value);
            }
            int count(const T &_value)
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.count(_value);
            }
            int size()
            {
                std::lock_guard<std::mutex> _(mtx);
                return data.size();
            }
            void clear()
            {
                std::lock_guard<std::mutex> _(mtx);
                data.clear();
            }
};
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

namespace System {
    namespace Config {
        namespace Event {
            namespace Distr {
                namespace Server {
                    namespace Modul {
                        namespace Harris {
                            namespace GPI {
                                class Config;
                            }
                        }
                    }
                }
            }
        }
    }
};
namespace System {
    namespace Comm {
        class Message;
        namespace Event {
            namespace Distr {
                class ChannelEvent;
            }
        }
    }
};

typedef System::Comm::Message system_msg_t;
typedef System::Comm::Event::Distr::ChannelEvent ch_evt_msg_t;

typedef System::Config::Event::Distr::Server::Modul::Harris::GPI::Config config_t;

class HarrisGPIServer
{
    private:
            mutable std::shared_mutex mtx_config;
            std::shared_ptr<config_t> configPtr;
            std::shared_ptr<config_t>& get_config();
            const std::shared_ptr<config_t>& get_config() const;
            bool set_config(const std::string &_json_str);
            
            std::string logger_uuid_;
            void set_logger_uuid(const std::string &_value);
            std::string logger_uuid() const;
            std::shared_ptr<spdlog::logger> log;
            virtual void create_logger();
                        
            std::atomic<bool> running_=true;
            void set_running(const bool &_value);
            bool running();
            
            std::atomic<bool> master_connected_=true;
            void set_master_connected(const bool &_value=true);
            bool master_connected();
            
            std::vector<std::thread> threads_;
            std::mutex mtx_threads_;
            std::vector<std::thread>& get_threads();
            
            std::mutex mtx_update_redis_database;
            ths_unordered_map<std::string,std::unique_ptr<std::mutex>> mutexes;
            ths_deque<std::string> redis_update_events;
            ths_deque<std::string> msg_broadcast_list;
                            
            std::thread t_data_sender_comm_thread;
                            
                            
            std::mutex redis_pub_mtx;
            std::condition_variable data_comm_sender_cv;
            void notify_data_sender_comm_thread();
                            
            bool read_file(const std::string &_file_path, std::string &_data, std::string &_errm, int &_errc);
           
            std::unordered_map<std::string, ch_evt_msg_t> ch_evt_msg_map_;
            bool init_trigger_mapping_table();
            
            void master_gpi_server_comm_thread();
            void backup_gpi_server_comm_thread();
            
            std::string timestamp();
            
            bool serialize_pbmsg_to_codedstream(const system_msg_t &_msg, char **_ackBuf, int &_bufsize);

    public:
            std::error_code ec;
            HarrisGPIServer();
            ~HarrisGPIServer();
            bool read_config_file(const std::string &_config_file_path);
            bool init();
            void run();
            void data_sender_comm_thread();
            void stop();
};
