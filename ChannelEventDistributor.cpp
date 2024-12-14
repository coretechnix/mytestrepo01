#include "ChannelEventDistributor.h"
#include "zcomm-publisher.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/util/json_util.h>
#include "System.Config.Event.Distr.Server.pb.h"
#include "System.Comm.pb.h"

#include <iomanip>
#include <algorithm>
#include <climits>
#include <string>
#include <fstream>
#include <sstream>

#include <zmq.h>

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/null_sink.h"
#include <spdlog/spdlog.h>

// Universally unique identifier
#include <uuid/uuid.h>

#define MAX_BUFFER_SIZE 131072
#define MAX_RECV_BUFF_SIZE 10485760 // 10 MB -> ### move to config

typedef System::Comm::Message system_msg_t;

typedef System::Comm::Distr::Port::Service::Request  psvc_req_msg_t;
typedef System::Comm::Distr::Port::Service::Response psvc_res_msg_t;
typedef System::Comm::Distr::Port::Service::PortList psvc_portlist_msg_t;
typedef System::Comm::Distr::Port::Service::msg_t    psvc_msg_t;

typedef std::chrono::steady_clock::time_point time_point_t;
typedef std::chrono::hours        hours_t;
typedef std::chrono::minutes      minutes_t;
typedef std::chrono::seconds      seconds_t;
typedef std::chrono::milliseconds milliseconds_t;

ChannelEventDistributor::ChannelEventDistributor()
{}
ChannelEventDistributor::~ChannelEventDistributor()
{
    #ifdef _C_DEBUG_
        google::protobuf::ShutdownProtobufLibrary();
    #endif
}
void ChannelEventDistributor::set_logger_uuid(const std::string &_value)
{
    logger_uuid_=_value;
}
std::string ChannelEventDistributor::logger_uuid() const
{
    return logger_uuid_;
}
void ChannelEventDistributor::create_logger()
{

    const std::string object_name = "chevtd";
    uuid_t id;
    uuid_generate(id);
    char *token = new char[100];
    uuid_unparse(id,token);
    set_logger_uuid( token );
    delete [] token;
    
    const std::string log_dir = get_config()->log_settings().log_dir();
    const std::string log_file = get_config()->log_settings().log_file();
    const std::string log_file_path = log_dir+( !log_dir.empty() ? (log_dir.back()!='/' ? "/" : "") : "./" )+( log_file.empty() ? logger_uuid() : log_file);
    const int log_max_file_size = get_config()->log_settings().log_max_file_size(); //MB
    const int log_max_num_of_files = get_config()->log_settings().log_max_num_of_files();

    try {
        if ( get_config()->log_settings().enabled() )
        {

            std::vector<spdlog::sink_ptr> sinks;
            if ( get_config()->log_settings().is_console_logger_enabled() )
            {
                sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
            }

            if ( get_config()->log_settings().is_file_logger_enabled() )
            {
                sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_file_path, 1048576 * log_max_file_size, log_max_num_of_files));
            }

            if ( !sinks.empty() )
            {
                log = std::make_shared<spdlog::logger>(logger_uuid(), begin(sinks), end(sinks));
            }
            else
            {
                log = spdlog::create<spdlog::sinks::null_sink_st>("null_logger");
            }
            /*
            log = spdlog::rotating_logger_mt(logger_uuid(), log_file_path, 1048576 * log_max_file_size, log_max_num_of_files);
            */
        }
        else
        {
            log = spdlog::create<spdlog::sinks::null_sink_st>("null_logger");
        }
        /*if ( get_config()->log_settings().enabled() )
        {
            log = spdlog::rotating_logger_mt(logger_uuid(), log_file_path, 1048576 * log_max_file_size, log_max_num_of_files);
        }
        else
        {
            log = spdlog::create<spdlog::sinks::null_sink_st>("null_logger");
        }*/
    }
    catch (const spdlog::spdlog_ex& ex)
    {
        printf("CHEVTD - ERROR - logger initialization failed! Trying to create console logger...\n");
        try {
            log = spdlog::stdout_color_mt("console");
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            printf("CHEVTD - ERROR - console logger initialization failed! Trying to create null logger...\n");
            try {
                log = spdlog::create<spdlog::sinks::null_sink_st>("null_logger");
            }
            catch (const spdlog::spdlog_ex& ex)
            {
                printf("CHEVTD - ERROR - null logger initialization failed!\n");
            }
        }
    }
    if (log)
    {
        log->set_level(spdlog::level::trace);
        log->flush_on(spdlog::level::trace);
        const std::string pattern = std::string("[%Y-%m-%d %T.%f]")+( log_file.empty() ? "["+object_name+"]" : "" )+"[%-8l] - %v";
        log->set_pattern( pattern );
        #ifdef _C_DEBUG_
        log->info("info - log test");
        log->warn("warn - log test");
        log->error("error - log test");
        log->critical("critical - log test");
        #endif
    }
}
const std::shared_ptr<config_t>& ChannelEventDistributor::get_config() const
{
  std::shared_lock<std::shared_mutex> _(mtx_config);
  return configPtr_;
}
std::shared_ptr<config_t>& ChannelEventDistributor::get_config()
{
  std::lock_guard<std::shared_mutex> _(mtx_config);
  return configPtr_;
}
std::vector<std::thread>& ChannelEventDistributor::get_threads()
{
    std::lock_guard<std::mutex> _(mtx_threads_);
    return threads_;
}
std::string ChannelEventDistributor::timestamp()
{
    using namespace std::chrono;
    using clock = system_clock;

    const auto current_time_point {clock::now()};
    const auto current_time {clock::to_time_t (current_time_point)};
    const auto current_localtime {*std::localtime (&current_time)};
    const auto current_time_since_epoch {current_time_point.time_since_epoch()};
    const auto current_milliseconds {duration_cast<milliseconds> (current_time_since_epoch).count() % 1000};

    std::ostringstream stream;
    stream << std::put_time (&current_localtime, "%T") << "." << std::setw (3) << std::setfill ('0') << current_milliseconds;
    return stream.str();
}
bool ChannelEventDistributor::read_config_file(const std::string& _config_file_path)
{
    #ifdef _C_DEBUG_
    //log()->debug( "{} - started...",__PRETTY_FUNCTION__ );
    #endif

    std::string errm;
    int errc;
    std::string json_data;

    #ifdef _C_DEBUG_
    //log()->debug( "{} - reading config file '{}'...",__PRETTY_FUNCTION__,_config_file_path);
    #endif

    if ( read_file(_config_file_path,json_data,errm,errc) )
    {
        #ifdef _C_DEBUG_
        //log()->debug( "{} - config file read OK (json_data size: {})",__PRETTY_FUNCTION__,json_data.size() );
        #endif

        get_config() = std::make_shared<config_t> ();
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields=true;

        #ifdef _C_DEBUG_
        //log()->debug( "{} - start to parse json data...",__PRETTY_FUNCTION__ );
        #endif

        auto status = google::protobuf::util::JsonStringToMessage(json_data, get_config().get(), options);

        #ifdef _C_DEBUG_
        //log()->debug( "{} - start to parse json data - done",__PRETTY_FUNCTION__ );
        #endif
        if ( status.ok() )
        {
            #ifdef _C_DEBUG_
            //log()->debug( "{} - '{}'",__PRETTY_FUNCTION__,get_config()->DebugString() );
            //log()->debug( "{} - JsonStringToMessage is Ok returning",__PRETTY_FUNCTION__ );
            #endif
            
            // if OK store the config file path
            set_config_file_path(_config_file_path);
            
            return true;
        }
        else
        {
            #ifdef _C_DEBUG_
            //log()->error( "{} - JsonStringToMessage() failed!",__PRETTY_FUNCTION__ );
            #else
            //log()->error( "Failed to parse configuration data (json format)!" );
            #endif
        }
    }
    else
    {
        #ifdef _C_DEBUG_
            //log()->error( "{} - Failed to read config file '{}' - errc: '{}', errm: '{}' ",__PRETTY_FUNCTION__,_config_file_path,errc
        #else
            //log()->error( "Failed to read config file '{}' - errc: '{}', errm: '{}' ",_config_file_path,errc,errm );
        #endif
    }
    #ifdef _C_DEBUG_
    //log()->debug( "{} - Something went wrong! Returning false...",__PRETTY_FUNCTION__ );
    #endif
    return false;    
}
bool ChannelEventDistributor::read_file(const std::string &_file_path, std::string &_data, std::string &_errm, int &_errc)
{
        _errm.clear();
        _errc=0;

        if ( !_file_path.empty() )
        {
            std::ifstream ifs;
            ifs.exceptions ( std::ifstream::badbit | std::ifstream::failbit );
            try
            {
                ifs.open ( _file_path.c_str(), std::ios_base::in | std::ios_base::binary );
                if ( ifs.is_open() )
                {
                    if ( !_data.empty() ) _data.clear();
                    ifs.seekg( 0,std::ios::end );
                    _data.reserve( ifs.tellg() );
                    ifs.seekg( 0,std::ios::beg );
                    _data.assign( (std::istreambuf_iterator<char>(ifs)),
                                   std::istreambuf_iterator<char>());
                    ifs.close();
                    return true;

                }
                _errm+="Failed to open file";
            }
            catch(const std::ios_base::failure& ex)
            {
                _errm+="Exception caught : ios_base::failure (opening/reading/closing file) -> "+std::string( ex.what() );
                _errc=ex.code().value();
            }
            catch (const std::out_of_range& ex)
            {
                _errm+="Exception caught : out_of_range -> "+std::string( ex.what() );
                _errc=-1;
            }
            catch (const std::length_error& ex)
            {
                _errm+="Exception caught : length_error -> "+std::string( ex.what() );
                _errc=-1;
            }
            catch (const std::bad_alloc& ex)
            {
                _errm+="Exception caught : bad_alloc -> "+std::string( ex.what() );
                _errc=-1;
            }
        }
        else
        {
            _errm+="Given file path is empty string!";
        }
        return false;
}
bool ChannelEventDistributor::save_file(const std::string &_file_path, const std::string &_data, std::string &_errm, int &_errc)
{
        _errm.clear();
        _errc=0;

        if ( !_file_path.empty()&&!_data.empty() )
        {
            std::ofstream dst;
            dst.exceptions ( std::ios_base::badbit | std::ios_base::failbit );
            try
            {
                dst.open ( _file_path.c_str(), std::ios_base::out | std::ios_base::binary );
                if ( dst.is_open() )
                {
                    dst << _data;
                    dst.close();
                    return true;
                }
                _errm+="Failed to open file";
            }
            catch(const std::ios_base::failure& ex)
            {
                _errm+="Exception caught (opening/reading/closing file): "+std::string( ex.what() );
                _errc=ex.code().value();
            }
        }
        else
        {
            _errm+="Given file path or data is empty string!";
        }
        return false;
}
void ChannelEventDistributor::set_config_file_path(const std::string &_value)
{
    config_file_path_ = _value;
}
std::string ChannelEventDistributor::config_file_path() const
{
    return config_file_path_;
}
bool ChannelEventDistributor::init_sch_dist_queues()
{
    if ( !get_config() ) return false;
    
    const int server_size = get_config()->mutable_publisher_servers()->server_size();
    if ( 0==server_size )
    {
        return false;
    }
    for(int i=0;i<server_size;i++)
    {
         pub_svr_t *svr = get_config()->mutable_publisher_servers()->mutable_server(i);
         
         const std::string chid = svr->chid();
         if ( svr->enabled() )
         {
             #ifdef _C_DEBUG_
             log->debug("{} - Creating distribution queue for chid '{}'",__PRETTY_FUNCTION__,chid);
             #endif
             auto &res = sch_dist_queues_[chid];
             res.bind_port_= get_distr_server_real_bind_port(svr,(i+1));
             res.ttl_=std::chrono::seconds( svr->ttl_sec() ); // ### zero means infinite - into config !!! +  ? default value for dynamic created new queues ?
             res.th_=std::thread (&ChannelEventDistributor::channel_event_publisher_server_thread,this,svr);
         }
         else
         {
             log->warn("Server is disabled! Distribution queue skipped for chid '%s'!",chid);
         }
    }

    // ### comment it
    update_chid_distr_server_portlist();
    
    #ifdef _C_DEBUG_
    log->debug("{} - Creating distribution queue for chid '{}'",__PRETTY_FUNCTION__,sch_dist_queues_.size());
    #endif
    return true;
}
bool ChannelEventDistributor::remove_sch_dist_queue(const std::string &_chid)
{
    std::lock_guard<std::mutex> _(mtx_remove_dq_);
    if (sch_dist_queues_.erase(_chid) == 1) return true;
    return false;
}
bool ChannelEventDistributor::init()
{
    create_logger();
    
    running_.store(true);
    
    if ( NULL==(zmq_ctx=zmq_ctx_new()) )
    {
        log->error("failed to create zmq contex!");
        // ### get and print errno w errmsg
        return false;
    }
    
    // initializing event queues and processor threads
    if ( !init_sch_dist_queues() )
    {
        log->error("event queue initialization failed!");
        return false;
    }

    get_threads().push_back( std::thread (&ChannelEventDistributor::channel_event_publisher_thread, this) );
    get_threads().push_back( std::thread (&ChannelEventDistributor::publisher_portsvc_server_thread, this) );
    get_threads().push_back( std::thread (&ChannelEventDistributor::internal_notify_publisher_thread, this) );
    get_threads().push_back( std::thread (&ChannelEventDistributor::internal_notify_subcriber_thread, this) );

    if ( get_config()->ha_settings().enabled() )
    {
        if (get_config()->ha_settings().tcp_check_server_enabled())
        {
            get_threads().push_back( std::thread (&ChannelEventDistributor::tcp_check_server_thread, this) );
        }
    }

    if ( get_config()->module_comm_server().tcp_enabled() )
    {
        get_threads().push_back( std::thread (&ChannelEventDistributor::module_tcp_comm_server_thread, this) );
    }

    if ( get_config()->module_comm_server().ipc_enabled() )
    {
        get_threads().push_back( std::thread (&ChannelEventDistributor::module_ipc_comm_server_thread, this) );
    }
    
    return true;
}
void ChannelEventDistributor::run()
{
    if ( !get_threads().empty() )
    {
        #ifdef _C_DEBUG_
        log->debug("{} - waiting for threads...",__PRETTY_FUNCTION__);
        #endif
        for (std::thread &th : get_threads())
        {
            if (th.joinable()) th.join();
        }
    }
    
    // join all channel server thread
    for (auto& it: sch_dist_queues_)
    {
        if (it.second.th_.joinable()) it.second.th_.join();
    }
    
    if ( -1==zmq_ctx_term(zmq_ctx) )
    {
        log->error("failed to terminate zmq context!");
    }
    
    if (log)
    {
        log->info("closing log");
        spdlog::drop( logger_uuid() );
        spdlog::shutdown();
    }
}
bool ChannelEventDistributor::terminate()
{
    
    if (running_.load())
    {
        running_.store(false);
    }

    int res = ::shutdown(*tcp_portsvc_server_socket_,SHUT_RDWR);
    #ifdef _C_DEBUG_
    log->debug("{} - tcp_portsvc_server_socket_({}) shutdown res:'{}':'{}'",__PRETTY_FUNCTION__,(int)(*tcp_portsvc_server_socket_),res,strerror(errno));
    #endif
    
    if ( get_config()->module_comm_server().tcp_enabled() ) // ### and it is really running...
    {    
        int res = ::shutdown(*tcp_modcomm_server_socket_,SHUT_RDWR);
        #ifdef _C_DEBUG_
        log->debug("{} - tcp_modcomm_server_socket_({}) shutdown res:'{}':'{}'",__PRETTY_FUNCTION__,(int)(*tcp_modcomm_server_socket_),res,strerror(errno));
        #endif
    }
    
    if ( get_config()->module_comm_server().ipc_enabled() ) // ### and it is really running...
    {
        int res = ::shutdown(*ipc_modcomm_server_socket_,SHUT_RDWR);
        #ifdef _C_DEBUG_
        log->debug("{} - ipc_modcomm_server_socket_({}) shutdown res:'{}':'{}'",__PRETTY_FUNCTION__,(int)(*ipc_modcomm_server_socket_),res,strerror(errno));
        #endif
    }

    if (get_config()->ha_settings().enabled())
    {
        if (get_config()->ha_settings().tcp_check_server_enabled())
        {
            res = ::shutdown(*tcp_check_server_socket_,SHUT_RDWR);
            #ifdef _C_DEBUG_
            log->debug("{} - tcp_check_server_socket_({}) shutdown res:'{}':'{}'",__PRETTY_FUNCTION__,(int)(*tcp_check_server_socket_),res,strerror(errno));
            #endif
        }
        // ### dbglog
    }
    // ### dbglog
        
    notify_publisher_thread();
    notify(); // ### rename it - internal port service change publisher threads
    
    // notify all channel server thread to wake up
    for (auto& it: sch_dist_queues_)
    {
        it.second.cv_.notify_one();
    }
    return true;
}
void ChannelEventDistributor::tcp_check_server_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    const std::string bind_address = get_config()->ha_settings().tcp_check_bind_addr();
    const int bind_port = get_config()->ha_settings().tcp_check_bind_port();

    SOCKET lListenSocket = INVALID_SOCKET;
    int iResult;
    // Create a SOCKET for connecting to server
    lListenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (lListenSocket != INVALID_SOCKET)
    {
        int reuseadd = 1;
        if ( -1==setsockopt(lListenSocket, SOL_SOCKET, SO_REUSEADDR, &reuseadd, sizeof(int)) )
        {
            #ifdef _C_DEBUG_
            log->error("{} - Failed to setsockopt: SO_REUSEADDR!",__PRETTY_FUNCTION__);
            #else
            log->error("[TcpCheck] Failed to setsockopt: SO_REUSEADDR!");
            #endif
        }
        struct sockaddr_in service;
        service.sin_family = AF_INET;
        service.sin_port = htons( bind_port );
        inet_pton(AF_INET, bind_address.c_str(), &service.sin_addr.s_addr);

        tcp_check_server_socket_ = &lListenSocket;
        iResult = ::bind(lListenSocket, (struct sockaddr*)&service, sizeof(service));
        if (iResult != SOCKET_ERROR)
        {
            iResult = ::listen(*tcp_check_server_socket_, SOMAXCONN);
            if (iResult != SOCKET_ERROR)
            {
                while (running_.load())
                {
                    SOCKET ClientSocket = INVALID_SOCKET;
                    sockaddr_in addr;
                    socklen_t addrlen = sizeof(addr);
                    ClientSocket = accept(*tcp_check_server_socket_, (struct sockaddr*)&addr, &addrlen);
                    if (ClientSocket != INVALID_SOCKET)
                    {
                        char ipstr[INET_ADDRSTRLEN];
                        memset (ipstr,'\0',INET_ADDRSTRLEN);
                        inet_ntop(AF_INET, &addr.sin_addr, (char*)ipstr, 16);

                        #ifdef _C_DEBUG_
                        log->info("{} - Accepted Connection from : '{}:{}'",__PRETTY_FUNCTION__,ipstr, addr.sin_port);
                        #endif
                        std::string host = std::string(ipstr) + ":" + std::to_string(addr.sin_port);
                        // ### gethostby name here
                        std::thread (&ChannelEventDistributor::TcpCheckClientConnectionHandler,this,ClientSocket,host).detach();
                    }
                }
                iResult = ::shutdown(*tcp_check_server_socket_, SHUT_RDWR);
                iResult = close(*tcp_check_server_socket_);
            }
            else
            {
                #ifdef _C_DEBUG_
                log->error("{} - Failed to listen on server socket: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
                #else
                log->error("[TcpCheck] Failed to listen on server socket: '{}':'{}'",errno,strerror(errno));
                #endif
                close(*tcp_check_server_socket_);

            }
        }
        else
        {
            #ifdef _C_DEBUG_
            log->error("{} - Socket bind failed with error: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
            #else
            log->error("[TcpCheck] Socket bind failed with error: '{}':'{}'",errno,strerror(errno));
            #endif
            close(*tcp_check_server_socket_);
        }
    }
    else
    {
        #ifdef _C_DEBUG_
        log->error("{} - Socket creation failed with error: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
        #else
        log->error("[TcpCheck] Socket creation failed with error: '{}':'{}'",errno,strerror(errno));
        #endif
    }
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void ChannelEventDistributor::TcpCheckClientConnectionHandler(SOCKET _ClientSocket, const std::string &_host)
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif

    int iResult=0;
    int TxBytes = send(_ClientSocket, "0", 1, 0);
    // ### debug log

    // Shutdown our socket
    #ifdef OS_WIN
    iResult = ::shutdown(_ClientSocket, SD_SEND);
    #else
    iResult = ::shutdown(_ClientSocket, SHUT_RDWR);
    #endif
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - result of socket shutdown: '{}'",__PRETTY_FUNCTION__,_host,iResult);
    #endif
    #ifdef OS_WIN
    iResult = closesocket(_ClientSocket);
    #else
    iResult = close(_ClientSocket);
    #endif
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - result of closesocket: '{}'",__PRETTY_FUNCTION__,_host,iResult);
    log->debug("{} - [{}] - done",__PRETTY_FUNCTION__,_host);
    #endif
}
void ChannelEventDistributor::module_tcp_comm_server_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    const std::string bind_address = get_config()->module_comm_server().tcp_bind_ip();
    const int bind_port = get_config()->module_comm_server().tcp_bind_port();
    
    SOCKET lListenSocket = INVALID_SOCKET;
    int iResult;
    // Create a SOCKET for connecting to server
    lListenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (lListenSocket != INVALID_SOCKET)
    {
        int reuseadd = 1;
        if ( -1==setsockopt(lListenSocket, SOL_SOCKET, SO_REUSEADDR, &reuseadd, sizeof(int)) )
        {
            #ifdef _C_DEBUG_
            log->error("{} - Failed to setsockopt: SO_REUSEADDR!",__PRETTY_FUNCTION__);
            #else
            log->error("[TcpCommSvr] Failed to setsockopt: SO_REUSEADDR!");
            #endif
        }
        struct sockaddr_in service;
        service.sin_family = AF_INET;
        service.sin_port = htons( bind_port );
        inet_pton(AF_INET, bind_address.c_str(), &service.sin_addr.s_addr);
        
        tcp_modcomm_server_socket_ = &lListenSocket;
        iResult = ::bind(lListenSocket, (struct sockaddr*)&service, sizeof(service));
        if (iResult != SOCKET_ERROR)
        {
            iResult = ::listen(*tcp_modcomm_server_socket_, SOMAXCONN);
            if (iResult != SOCKET_ERROR)
            {
                while (running_.load())
                {
                    SOCKET ClientSocket = INVALID_SOCKET;
                    sockaddr_in addr;
                    socklen_t addrlen = sizeof(addr);
                    ClientSocket = accept(*tcp_modcomm_server_socket_, (struct sockaddr*)&addr, &addrlen);
                    if (ClientSocket != INVALID_SOCKET)
                    {
                        char ipstr[INET_ADDRSTRLEN];
                        inet_ntop(AF_INET, &addr.sin_addr, (char*)ipstr, 16);
                        #ifdef _C_DEBUG_
                        log->info("{} - Accepted Connection from : '{}:{}'",__PRETTY_FUNCTION__,ipstr,addr.sin_port);
                        #else
                        log->info("[TcpCommSvr] Accepted Connection from : '{}:{}'",ipstr,addr.sin_port);
                        #endif
                        std::string host = std::string(ipstr) + ":" + std::to_string(addr.sin_port);
                        // ### gethostby name here
                        std::thread t( &ChannelEventDistributor::ClientConnectionHandler,this,ClientSocket,host);
                        t.detach();
                        //std::thread (&ChannelScheduleDistributor::ClientConnectionHandler,this,ClientSocket,host).detach();
                    }
                }
                iResult = ::shutdown(*tcp_modcomm_server_socket_, SHUT_RDWR);
                iResult = close(*tcp_modcomm_server_socket_);
            }
            else
            {
                #ifdef _C_DEBUG_
                log->error("{} - Failed to listen on server socket: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
                #else
                log->error("[TcpCommSvr] Failed to listen on server socket: '{}':'{}'",errno,strerror(errno));
                #endif
                close(*tcp_modcomm_server_socket_);
            }
        }
        else
        {
            #ifdef _C_DEBUG_
            log->error("{} - Socket bind failed with error: '{}':'{}''",__PRETTY_FUNCTION__,errno,strerror(errno));
            #else
            log->error("[TcpCommSvr] Socket bind failed with error: '{}':'{}'",errno,strerror(errno));
            #endif
            close(*tcp_modcomm_server_socket_);
        }
    }
    else
    {
        #ifdef _C_DEBUG_
        log->error("{} - Socket creation failed with error: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
        #else
        log->error("[TcpCommSvr] Socket creation failed with error: '{}':'{}'",errno,strerror(errno));
        #endif
    }    
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void ChannelEventDistributor::module_ipc_comm_server_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    // ### On Linux, sun_path is 108 bytes in size - must check it
    const std::string bind_address = get_config()->module_comm_server().ipc_bind_address();
    
    struct sockaddr_un local;

    SOCKET lListenSocket = INVALID_SOCKET;
    int iResult;
    lListenSocket = socket(AF_UNIX, SOCK_STREAM, 0);
    
    if (lListenSocket != INVALID_SOCKET)
    {
        local.sun_family = AF_UNIX;
        strcpy( local.sun_path, bind_address.c_str() );
        unlink(local.sun_path);
        
        ipc_modcomm_server_socket_ = &lListenSocket;
        iResult = ::bind(*ipc_modcomm_server_socket_, (struct sockaddr *)&local,sizeof(local));
        
        if (iResult != SOCKET_ERROR)
        {
            iResult = ::listen(*ipc_modcomm_server_socket_, SOMAXCONN);
            if (iResult != SOCKET_ERROR)
            {
                while (running_.load())
                {
                    SOCKET ClientSocket = INVALID_SOCKET;
                    struct sockaddr_un addr;
                    socklen_t addrlen = sizeof(addr);
                    ClientSocket = accept(*ipc_modcomm_server_socket_, (struct sockaddr *)&addr,(socklen_t*)&addrlen);
                    
                    if (ClientSocket != INVALID_SOCKET)
                    {
                        #ifdef _C_DEBUG_
                        log->info("{} - Accepted Connection from : '{}'",__PRETTY_FUNCTION__,addr.sun_path+1);
                        #else
                        log->info("[IpcCommSvr] Accepted Connection from : '{}'",addr.sun_path+1);
                        #endif
                        std::string host = std::string(addr.sun_path);
                        std::thread t( &ChannelEventDistributor::ClientConnectionHandler,this,ClientSocket,host);
                        t.detach();
                    }
                }
                iResult = ::shutdown(*ipc_modcomm_server_socket_, SHUT_RDWR);
                iResult = close(*ipc_modcomm_server_socket_);
            }
            else
            {
                #ifdef _C_DEBUG_
                log->error("{} - Failed to listen on server socket: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
                #else
                log->error("[IpcCommSvr] Failed to listen on server socket: '{}':'{}'",errno,strerror(errno));
                #endif
                close(*ipc_modcomm_server_socket_);
            }
            
            unlink(local.sun_path);
            
        }
        else
        {
            #ifdef _C_DEBUG_
            log->error("{} - Socket bind failed with error: '{}':'{}''",__PRETTY_FUNCTION__,errno,strerror(errno));
            #else
            log->error("[IpcCommSvr] Socket bind failed with error: '{}':'{}'",errno,strerror(errno));
            #endif
            close(*ipc_modcomm_server_socket_);
        }
    }
    else
    {
        #ifdef _C_DEBUG_
        log->error("{} - Socket creation failed with error: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
        #else
        log->error("[IpcCommSvr] Socket creation failed with error: '{}':'{}'",errno,strerror(errno));
        #endif
    }    
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void ChannelEventDistributor::publisher_portsvc_server_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    const std::string bind_address = get_config()->port_service().bind_ip();
    const int bind_port = get_config()->port_service().bind_port();
    
    SOCKET lListenSocket = INVALID_SOCKET;
    int iResult;
    lListenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (lListenSocket != INVALID_SOCKET)
    {
        int reuseadd = 1;
        if ( -1==setsockopt(lListenSocket, SOL_SOCKET, SO_REUSEADDR, &reuseadd, sizeof(int)) )
        {
            #ifdef _C_DEBUG_
            log->error("{} - Failed to setsockopt: SO_REUSEADDR!",__PRETTY_FUNCTION__);
            #else
            log->error("[PortSvcSvr] Failed to setsockopt: SO_REUSEADDR!");
            #endif
        }
        struct sockaddr_in service;
        service.sin_family = AF_INET;
        service.sin_port = htons( bind_port );
        inet_pton(AF_INET, bind_address.c_str(), &service.sin_addr.s_addr);
        
        tcp_portsvc_server_socket_ = &lListenSocket;
        iResult = ::bind(lListenSocket, (struct sockaddr*)&service, sizeof(service));
        if (iResult != SOCKET_ERROR)
        {
            iResult = ::listen(*tcp_portsvc_server_socket_, SOMAXCONN);
            if (iResult != SOCKET_ERROR)
            {
                while (running_.load())
                {
                    SOCKET ClientSocket = INVALID_SOCKET;
                    sockaddr_in addr;
                    socklen_t addrlen = sizeof(addr);
                    ClientSocket = accept(*tcp_portsvc_server_socket_, (struct sockaddr*)&addr, &addrlen);
                    if (ClientSocket != INVALID_SOCKET)
                    {
                        char ipstr[INET_ADDRSTRLEN];
                        inet_ntop(AF_INET, &addr.sin_addr, (char*)ipstr, 16);
                        #ifdef _C_DEBUG_
                        log->info("{} - Accepted Connection from : '{}:{}'",__PRETTY_FUNCTION__,ipstr,addr.sin_port);
                        #else
                        log->info("[PortSvcSvr] Accepted Connection from : '{}:{}'",ipstr,addr.sin_port);
                        #endif
                        std::string host = std::string(ipstr) + ":" + std::to_string(addr.sin_port);
                        // ### gethostby name here
                        std::thread t( &ChannelEventDistributor::PortSvcClientConnectionHandler,this,ClientSocket,host);
                        t.detach();
                    }
                }
                iResult = ::shutdown(*tcp_portsvc_server_socket_, SHUT_RDWR);
                iResult = close(*tcp_portsvc_server_socket_);
            }
            else
            {
                #ifdef _C_DEBUG_
                log->error("{} - Failed to listen on server socket: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
                #else
                log->error("[PortSvcSvr] Failed to listen on server socket: '{}':'{}'",errno,strerror(errno));
                #endif
                close(*tcp_portsvc_server_socket_);
            }
        }
        else
        {
            #ifdef _C_DEBUG_
            log->error("{} - Socket bind failed with error: '{}':'{}''",__PRETTY_FUNCTION__,errno,strerror(errno));
            #else
            log->error("[PortSvcSvr] Socket bind failed with error: '{}':'{}'",errno,strerror(errno));
            #endif
            close(*tcp_portsvc_server_socket_);
        }
    }
    else
    {
        #ifdef _C_DEBUG_
        log->error("{} - Socket creation failed with error: '{}':'{}'",__PRETTY_FUNCTION__,errno,strerror(errno));
        #else
        log->error("[PortSvcSvr] Socket creation failed with error: '{}':'{}'",errno,strerror(errno));
        #endif
    }
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
// Module client app
void ChannelEventDistributor::ClientConnectionHandler(SOCKET _ClientSocket, const std::string &_host)
{
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - started",__PRETTY_FUNCTION__,_host);
    #endif
    int iResult=0;
    int recv_failcnt = 0; // ### variable for count invalid messages incoming - create config entry for this in API section: drop_conn_after_n_failures, max_nbr_of_failures
    
    char * buffer = new char[MAX_RECV_BUFF_SIZE]();
    int actMsgSize=0;
    int RxBuffSize=0;
        
    while (running_.load())
    {
        #ifdef _C_DEBUG_
        log->debug("{} - [{}] - waiting for message...",__PRETTY_FUNCTION__,_host);
        #endif
        if ( !RxBuffSize ) bzero(buffer,MAX_RECV_BUFF_SIZE);
        int RxBytes = read(_ClientSocket, buffer+RxBuffSize, MAX_RECV_BUFF_SIZE);
        if (0<RxBytes)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Received '{}' bytes of data...",__PRETTY_FUNCTION__,_host,RxBytes);
            #endif
            actMsgSize=RxBuffSize+RxBytes;
            system_msg_t msg;
            google::protobuf::io::ArrayInputStream arrayIn(buffer, actMsgSize);
            google::protobuf::io::CodedInputStream codedIn(&arrayIn);
            google::protobuf::uint32 size=0;
            if ( codedIn.ReadVarint32(&size) )
            {
                google::protobuf::io::CodedInputStream::Limit msgLimit = codedIn.PushLimit(size);
                if ( msg.ParseFromCodedStream(&codedIn) && codedIn.ConsumedEntireMessage() )
                {
                    #ifdef _C_DEBUG_
                    log->debug("{} - [{}] - Got entire message(RxBuffSize: '{}')...",__PRETTY_FUNCTION__,_host,actMsgSize);
                    #endif
                    if( msg.has_channel_event() )
                    {
                        const std::string chid = msg.mutable_channel_event()->chid();
                        #ifdef _C_DEBUG_
                        log->debug("{} - [{}] - has_channel_event()(chid:'{}') - OK",__PRETTY_FUNCTION__,_host,chid);
                        #else
                        log->info("[host:'{}'] - got channel event: '{}'",_host,msg.channel_event().ShortDebugString());
                        #endif
                        
                        // for all messages port
                        chsch_msg_list_.push( msg ) ;
                        notify_publisher_thread();
                        
                         // chid specific publisher
                         sch_dist_queues_.find( chid );
                         auto it = sch_dist_queues_.find( chid );
                         if ( sch_dist_queues_.end()!=it )
                         {
                             #ifdef _C_DEBUG_
                             log->debug("{} - [{}] - Adding sch msg by chid '{}'",__PRETTY_FUNCTION__,_host,chid);
                             #endif
                             it->second.q_.push( msg );
                             sch_dist_queues_[chid].cv_.notify_one();
                         }
                         else
                         {
                             log->warn("[host:'{}'] - chid not found in 'sch_dist_queues_'! Skipped...",_host);
                             if ( get_config()->publisher_servers().distr_server_dynamic_creaton_for_unknown_chids_enabled() )
                             {
                                 #ifdef _C_DEBUG_
                                 log->debug("{} - [{}] - dynamic server creation enabled...",__PRETTY_FUNCTION__,_host);
                                 #endif
                                 if ( add_new_distr_queue(chid) )
                                 {
                                     log->info("[host:'{}'] - Creating distribution queue for chid '{}'",_host,chid);
                                     
                                     pub_svr_t *svr;
                                     int idx = add_new_pub_svr_to_config(chid,&svr);
                                     if (0<idx)
                                     {
                                         #ifdef _C_DEBUG_
                                         log->debug("{} - [{}] - new server added to config, index is '{}'",__PRETTY_FUNCTION__,_host,idx);
                                         #endif
                                         const bool dalloc=true;
                                         auto &res = sch_dist_queues_[chid];
                                         res.bind_port_= get_distr_server_real_bind_port(svr,idx,dalloc); // we are using this idx from 1 only, so it is needed to increment by 1 ### NOT TRUE remove comment?
                                         res.ttl_=std::chrono::seconds( svr->ttl_sec() );
                                         res.th_=std::thread (&ChannelEventDistributor::channel_event_publisher_server_thread,this,svr);
                                         
                                         // notify port service listeners that we have new distr server in the list
                                         notify();
                                         
                                         // ### add the message to the queue 2-3x times with some delay ??? -> we have to receive the first message everywhere ### LAST
                                         
                                     }
                                     else
                                     {
                                         log->error("[host:'{}'] - failed to add new server entry into config, index is '0'",_host);
                                     }
                                 }
                                 else
                                 {
                                     // ### failed to add new queue
                                 }
                             }
                             #ifdef _C_DEBUG_
                             else
                             {
                                 log->debug("{} - [{}] - dynamic server creation disabled!",__PRETTY_FUNCTION__,_host);
                             }
                             #endif
                         }
                        codedIn.PopLimit(msgLimit);
                    }
                    else
                    {
                        log->error("[host:'{}'] - message has no 'channel event2 frame!",_host);
                    }
                    
                    RxBuffSize=0;
                }
                else
                {
                    #ifdef _C_DEBUG_
                    log->debug("{} - [{}] - failed to parse from coded stream!(this is just a part of the entire message)",__PRETTY_FUNCTION__,_host);
                    #endif
                    RxBuffSize+=RxBytes;
                }
            }
            #ifdef _C_DEBUG_
            else
            {
                log->error("[host:'{}'] - failed to ReadVarin32 msg size!",_host);
            }
            #endif
            actMsgSize=0;
        }
        else if (0==RxBytes)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Received '{}' bytes of data...(client has gone)",__PRETTY_FUNCTION__,_host,RxBytes);
            #else
            log->info("[host:'{}'] - client is leaving...",_host);
            #endif
            break;
        }
    }
    
    delete [] buffer;
    
    log->info("[host:'{}'] - client has left",_host);
    #ifdef OS_WIN
    iResult = ::shutdown(_ClientSocket, SD_SEND);
    #else
    iResult = ::shutdown(_ClientSocket, SHUT_RDWR);
    #endif
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - result of socket shutdown: '{}'",__PRETTY_FUNCTION__,_host,iResult);
    #endif
    #ifdef OS_WIN
    iResult = closesocket(_ClientSocket);
    #else
    iResult = close(_ClientSocket);
    #endif
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - result of closesocket: '{}'",__PRETTY_FUNCTION__,_host,iResult);
    log->debug("{} - [{}] - done",__PRETTY_FUNCTION__,_host);
    #endif
}
// Port list getter client apps
void ChannelEventDistributor::PortSvcClientConnectionHandler(SOCKET _ClientSocket, const std::string &_host)
{
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - started",__PRETTY_FUNCTION__,_host);
    #endif
    // send portlist to the connected client
    system_msg_t msg;
    create_port_svc_msg(msg);
    char* ackBuf;
    int bufsize;
    if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
    {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Message successfully serialized to codedStream",__PRETTY_FUNCTION__,_host);
            log->debug("{} - [{}] - Encapsulated protobuf msg size: '{}'",__PRETTY_FUNCTION__,_host,bufsize);
            #endif
            int TxBytes = send(_ClientSocket, ackBuf, bufsize, 0);
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Number of bytes sent: '{}'",__PRETTY_FUNCTION__,_host,TxBytes);
            #endif
    }
    delete [] ackBuf;

    // === create internal notify subscriber socket =============================================================================
    void *socket = zmq_socket (zmq_ctx, ZMQ_SUB);
    int i_linger=0;
    int rc = zmq_setsockopt (socket, ZMQ_LINGER, &i_linger, sizeof i_linger);
    // subscribe to everything
    rc = zmq_setsockopt (socket, ZMQ_SUBSCRIBE, "", 0);
    rc = zmq_connect (socket, inproc_bind_address.c_str());
    // =================================================================================================================
    
    // LINUX - Set timeout on the socket
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(_ClientSocket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    int iResult=0;
    int recv_failcnt = 0; // ### variable for count invalid messages incoming - create config entry for this in API section: drop_conn_after_n_failures, max_nbr_of_failures
    
    char * buffer = new char[MAX_RECV_BUFF_SIZE]();
    int actMsgSize=0;
    int RxBuffSize=0;

    bool notified=false;
    while (running_.load())
    {
        // === internal pub socket =======================================================================================
        char buf [256];
        memset (buf,'\0',256);
        int RxBytes=0;
        if ( 0<(RxBytes=zmq_recv (socket, buf, 256, ZMQ_DONTWAIT)) )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Received '{}' bytes by inproc: '{}'",__PRETTY_FUNCTION__,_host,RxBytes,buf);
            #endif
            notified=true;
        }
        // ===============================================================================================================
        #ifdef _C_DEBUG_
        log->debug("{} - [{}] - Waiting for message...",__PRETTY_FUNCTION__,_host);
        #endif
        if ( !RxBuffSize ) bzero(buffer,MAX_RECV_BUFF_SIZE);
        RxBytes = read(_ClientSocket, buffer+RxBuffSize, MAX_RECV_BUFF_SIZE);
        if (0<RxBytes)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Received '{}' bytes of data...",__PRETTY_FUNCTION__,_host,RxBytes);
            #endif
            actMsgSize=RxBuffSize+RxBytes;
            msg.Clear();
            google::protobuf::io::ArrayInputStream arrayIn(buffer, actMsgSize);
            google::protobuf::io::CodedInputStream codedIn(&arrayIn);
            google::protobuf::uint32 size=0;
            if ( codedIn.ReadVarint32(&size) )
            {
                google::protobuf::io::CodedInputStream::Limit msgLimit = codedIn.PushLimit(size);
                if ( msg.ParseFromCodedStream(&codedIn) && codedIn.ConsumedEntireMessage() )
                {
                    #ifdef _C_DEBUG_
                    log->debug("{} - [{}] - Got entire message(RxBuffSize: '{}')...",__PRETTY_FUNCTION__,_host,actMsgSize);
                    #endif
                    if (notified)
                    {
                        #ifdef _C_DEBUG_
                        log->debug("{} - [{}] - It has been notified, sending update to client...(notified:'{}')",__PRETTY_FUNCTION__,_host,notified);
                        #endif
                        // ---------------------------------------------------------------------------------------
                        create_port_svc_msg(msg);
                        char* ackBuf;
                        int bufsize;
                        if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                        {
                            #ifdef _C_DEBUG_
                            log->debug("{} - [{}] - Message successfully serialized to codedStream",__PRETTY_FUNCTION__,_host);
                            log->debug("{} - [{}] - Encapsulated protobuf msg size: '{}'",__PRETTY_FUNCTION__,_host,bufsize);
                            #endif
                            int TxBytes = send(_ClientSocket, ackBuf, bufsize, 0);
                            #ifdef _C_DEBUG_
                            log->debug("{} - [{}] - Number of bytes sent: '{}'",__PRETTY_FUNCTION__,_host,TxBytes);
                            #endif
                        }
                        delete [] ackBuf;
                        //----------------------------------------------------------------------------------------
                        notified=false;
                        #ifdef _C_DEBUG_
                        log->debug("{} - [{}] - notified is now :'{}'",__PRETTY_FUNCTION__,_host,notified);
                        #endif
                    }
                    else
                    {
                    
                        if( msg.has_port_svc_msg() )
                        {
                            codedIn.PopLimit(msgLimit);
                            if ( msg.port_svc_msg().has_req() )
                            {
                                #ifdef _C_DEBUG_
                                log->debug("{} - [{}] - has_port_svc_msg().has_req() - OK",__PRETTY_FUNCTION__,_host);
                                log->debug("{} - [{}] - '{}'",__PRETTY_FUNCTION__,_host,msg.ShortDebugString());
                                #endif
                                if ( psvc_msg_t::PING == msg.port_svc_msg().req().type() )
                                {
                                    msg.Clear();
                                    psvc_res_msg_t *res = msg.mutable_port_svc_msg()->mutable_res();
                                    res->set_type(psvc_msg_t::PONG);
                                    char* ackBuf;
                                    int bufsize;
                                    if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                                    {
                                        int TxBytes = send(_ClientSocket, ackBuf, bufsize, 0);
                                        #ifdef _C_DEBUG_
                                        log->debug("{} - [{}] - Number of bytes sent: '{}'",__PRETTY_FUNCTION__,_host,TxBytes);
                                        #endif
                                    }
                                    #ifdef _C_DEBUG_
                                    else
                                    {
                                        log->debug("{} - [{}] - Failed to serialize message to codedStream!",__PRETTY_FUNCTION__,_host);
                                    }
                                    #endif
                                    delete [] ackBuf;
                                    
                                }
                                else
                                {
                                    log->error("[PortSvc:{}] - request type is not PING",_host);
                                }
                            }
                            else
                            {
                                log->error("[PortSvc:{}] - message has no port service request!",_host);
                            }
                        }
                        else
                        {
                            log->error("[PortSvc:{}] - Received message has no 'port_svc_msg' frame!",_host);
                        }                    
                    }//if (notified)
                    
                    RxBuffSize=0;
                }
                else
                {
                    log->error("[PortSvc:{}] - Failed to parse from coded stream!(this is just a part of the entire message)",_host);
                    RxBuffSize+=RxBytes;
                }
            }
            else
            {
                log->error("[PortSvc:{}] - Failed to ReadVarin32 msg size...",_host);
            }
            actMsgSize=0;
        }
        else if (0>RxBytes)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - recv timed out... (checking is notified)...",__PRETTY_FUNCTION__,_host);
            #endif
            if (notified)
            {
                #ifdef _C_DEBUG_
                log->debug("{} - [{}] - it has been notified, sending update to client...(notified:'{}'))",__PRETTY_FUNCTION__,_host,notified);
                #endif
                //--------------------------------------------------------------------------------------
                create_port_svc_msg(msg);
                char* ackBuf;
                int bufsize;
                if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                {
                    #ifdef _C_DEBUG_
                    log->debug("{} - [{}] - Message successfully serialized to codedStream",__PRETTY_FUNCTION__,_host);
                    log->debug("{} - [{}] - Encapsulated protobuf msg size: '{}'",__PRETTY_FUNCTION__,_host,bufsize);
                    #endif
                    int TxBytes = send(_ClientSocket, ackBuf, bufsize, 0);
                    #ifdef _C_DEBUG_
                    log->debug("{} - [{}] - Number of bytes sent: '{}'",__PRETTY_FUNCTION__,_host,TxBytes);
                    #endif
                }
                delete [] ackBuf;
                //--------------------------------------------------------------------------------------
                notified=false;
                #ifdef _C_DEBUG_
                log->debug("{} - [{}] - notified is now :'{}'",__PRETTY_FUNCTION__,_host,notified);
                #endif
            }
        }
        else if (0==RxBytes)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - [{}] - Received '{}' bytes of data...(client has gone)",__PRETTY_FUNCTION__,_host,RxBytes);
            #else
            log->info("[host:'{}'] - client is leaving...",_host);
            #endif
            break;
        }
    }
    
    delete [] buffer;
    
    log->info("[host:'{}'] - client has left",_host);
    #ifdef OS_WIN
    iResult = ::shutdown(_ClientSocket, SD_SEND);
    #else
    iResult = ::shutdown(_ClientSocket, SHUT_RDWR);
    #endif
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - result of socket shutdown: '{}'",__PRETTY_FUNCTION__,_host,iResult);
    #endif
    #ifdef OS_WIN
    iResult = closesocket(_ClientSocket);
    #else
    iResult = close(_ClientSocket);
    #endif
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - result of closesocket: '{}'",__PRETTY_FUNCTION__,_host,iResult);
    #endif
    
    rc = zmq_disconnect (socket, inproc_bind_address.c_str());
    zmq_close(socket);
    
    #ifdef _C_DEBUG_
    log->debug("{} - [{}] - done",__PRETTY_FUNCTION__,_host);
    #endif
}
void ChannelEventDistributor::channel_event_publisher_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    const std::string bind_ip = get_config()->publisher_server().bind_ip();
    const std::string bind_port = std::to_string( get_config()->publisher_server().bind_port() );
    void* context=zmq_ctx_new();
    void *socket = zmq_socket (context, ZMQ_PUB);
    int i_linger=0;
    int rc = zmq_setsockopt (socket, ZMQ_LINGER, &i_linger, sizeof i_linger);
    int i_sndhwm=0; // A value of zero means no limit.
    rc = zmq_setsockopt (socket, ZMQ_SNDHWM, &i_sndhwm, sizeof i_sndhwm);
    const std::string connection_string = "tcp://"+bind_ip+":"+bind_port;
    rc = zmq_bind (socket, connection_string.c_str());
    while (running_.load())
    {
        std::unique_lock<std::mutex> lk(mtx_cv_publisher_);

        auto now = std::chrono::system_clock::now();
        if( cv_publisher_.wait_until(lk, now + std::chrono::seconds(1), [this](){ return !chsch_msg_list_.empty(); }) )
        {
            lk.unlock();

            if ( running_.load() )
            {
                //while ( !chsch_msg_list_.empty() )
                //{
                        system_msg_t msg = chsch_msg_list_.front();
                        char* ackBuf;
                        int bufsize;
                        if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                        {
                            int TxBytes=0;
                            if ( 0<(TxBytes=zmq_send (socket, ackBuf, bufsize, 0)) )
                            {
                                chsch_msg_list_.pop();
                                #ifdef _C_DEBUG_
                                log->debug("{} - Message sent successfully({})! Last message popped out...",__PRETTY_FUNCTION__,TxBytes);
                                #endif
                            }
                        }
                        else
                        {
                            log->error("{} - Failed to serialize message to coded stream!",__PRETTY_FUNCTION__);
                        }
                        delete [] ackBuf;
            }
            #ifdef _C_DEBUG_
            else
            {
                log->debug("{} - Shutdown event! Notification skipped!",__PRETTY_FUNCTION__);
            }
            #endif
        }
        #ifdef _C_DEBUG_
        else
        {
            log->debug("{} - Condition variable timed out...",__PRETTY_FUNCTION__);
        }
        #endif
    }
    rc = zmq_unbind (socket, connection_string.c_str());
    zmq_close(socket);
    zmq_ctx_term(context);
    
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void ChannelEventDistributor::channel_event_publisher_server_thread(pub_svr_t *_svr)
{
    #ifdef _C_DEBUG_
    log->debug("{} - {} - started",__PRETTY_FUNCTION__,_svr->chid());
    #endif
    time_point_t tp_last_activity = std::chrono::steady_clock::now();
    const std::string chid = _svr->chid();
    const std::string bind_ip = _svr->bind_ip();
    const std::string bind_port = std::to_string( sch_dist_queues_[chid].bind_port_ );
    const bool test_mode = _svr->test_mode();
    seconds_t queue_ttl_sec = sch_dist_queues_[chid].ttl_;
    
    #ifdef _C_DEBUG_
    log->debug("{} - {} - starting server with bind address: '{}:{}'",__PRETTY_FUNCTION__,chid,bind_ip,bind_port);
    #endif
    
    void* context=zmq_ctx_new();
    void *socket = zmq_socket (context, ZMQ_PUB);
    int i_linger=0;
    int rc = zmq_setsockopt (socket, ZMQ_LINGER, &i_linger, sizeof i_linger);
    int i_sndhwm=0; // A value of zero means no limit.
    rc = zmq_setsockopt (socket, ZMQ_SNDHWM, &i_sndhwm, sizeof i_sndhwm);
    const std::string connection_string = "tcp://"+bind_ip+":"+bind_port;
    rc = zmq_bind (socket, connection_string.c_str());
    if (0>rc)
    {
        log->error("[dsvr:{}] Failed to bind server to address '{}' (bind port:{})...",chid,connection_string,bind_port);
    }
    while (running_.load())
    {
        std::unique_lock<std::mutex> lk(sch_dist_queues_[chid].mtx_);

        auto now = std::chrono::system_clock::now();
        if( sch_dist_queues_[chid].cv_.wait_until(lk, now + std::chrono::seconds(1), [this,chid](){ return !sch_dist_queues_[chid].q_.empty(); }) )
        {
            lk.unlock();

            if ( running_.load() )
            {
                //while ( !sch_dist_queues_[chid].q_.empty() )
                //{
                        system_msg_t msg = sch_dist_queues_[chid].q_.front();
                        char* ackBuf;
                        int bufsize;
                        if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                        {
                            int TxBytes=0;
                            if ( 0<(TxBytes=zmq_send (socket, ackBuf, bufsize, 0)) )
                            {
                                sch_dist_queues_[chid].q_.pop();
                                #ifdef _C_DEBUG_
                                log->debug("{} - {} - Message sent successfully({})! Last message popped out...",__PRETTY_FUNCTION__,chid,TxBytes);
                                #endif
                            }
                        }
                        else
                        {
                            log->error("{} - Schedule list publish failed! Failed to serialize message to coded stream!",__PRETTY_FUNCTION__);
                        }
                        delete [] ackBuf;
                // update activity time_point
                tp_last_activity = std::chrono::steady_clock::now();
            }
            #ifdef _C_DEBUG_
            else
            {
                log->debug("{} - {} - Shutdown event! Notification skipped!",__PRETTY_FUNCTION__,chid);
            }
            #endif
        }
        else
        {
            //timeout
            if (std::chrono::seconds::zero()!=queue_ttl_sec)
            {
                #ifdef _C_DEBUG_
                log->debug("{} - {} - ttl_ value is not zero, checking ttl value...",__PRETTY_FUNCTION__,chid);
                #endif
                time_point_t tp_now = std::chrono::steady_clock::now();
                auto d_elapsed = tp_now - tp_last_activity;
                seconds_t s = std::chrono::duration_cast<seconds_t> (d_elapsed);
                #ifdef _C_DEBUG_
                log->debug("{} - {} - thread has been running for '{}' seconds (ttl is set to '{}')",__PRETTY_FUNCTION__,chid,s.count(),queue_ttl_sec.count());
                #endif
                
                if (queue_ttl_sec<=s)
                {
                    if ( get_config()->publisher_servers().change_ttl_to_infinite_after_timeout_enabled() )
                    {
                        sch_dist_queues_[chid].ttl_=std::chrono::seconds::zero();
                        queue_ttl_sec=std::chrono::seconds::zero();;
                        _svr->set_ttl_sec(0);
                        update_config_file();
                    }
                    else
                    {
                        #ifdef _C_DEBUG_
                        log->debug("{} - {} - distribution queue has reached the ttl value, removing the queue and freeing up all resources...",__PRETTY_FUNCTION__,chid);
                        #else
                        log->info("[dsvr-{}] - distribution queue has reached the ttl value, removing the queue and freeing up all resources...",chid);
                        #endif
                        
                        sch_dist_queues_[chid].th_.detach(); // must detach in this case, we can't join this anymore

                        #ifdef _C_DEBUG_
                        log->debug("{} - {} - adding port '{}' to the reusable ports (ccq_reusable_ports_.size:'{}')...",__PRETTY_FUNCTION__,chid,sch_dist_queues_[chid].bind_port_,ccq_reusable_ports_.size());
                        #endif
                        ccq_reusable_ports_.push(sch_dist_queues_[chid].bind_port_);
                        #ifdef _C_DEBUG_
                        log->debug("{} - {} - ccq_reusable_ports_.size:'{}'..",__PRETTY_FUNCTION__,chid,ccq_reusable_ports_.size());
                        #endif
                        if ( remove_sch_dist_queue(chid) )
                        {
                            #ifdef _C_DEBUG_
                            log->debug("{} - {} - distribution queue has been successfully removed",__PRETTY_FUNCTION__,chid);
                            #else
                            log->info("[dsvr-{}] - distribution queue has been successfully removed",chid);
                            #endif

                            // ### move some better place
                            remove_pub_svr_from_config(chid);
                            #ifdef _C_DEBUG_
                            log->debug("{} - {} - config entry has been successfully removed",__PRETTY_FUNCTION__,chid);
                            #else
                            log->info("[dsvr-{}] - config entry has been successfully removed",chid);
                            #endif
                        }
                        // notify port service listeners about the change - one server has gone
                        notify();
                        break;
                    }
                }
            }
            #ifdef _C_DEBUG_
            else
            {
                log->debug("{} - {} - ttl is zero - running infinite...",__PRETTY_FUNCTION__,chid);
            }
            #endif
        }
    }
    rc = zmq_unbind (socket, connection_string.c_str());
    zmq_close(socket);
    zmq_ctx_term(context);
    
    #ifdef _C_DEBUG_
    log->debug("{} - {} - done",__PRETTY_FUNCTION__,chid);
    #endif
}
void ChannelEventDistributor::notify_publisher_thread()
{
    cv_publisher_.notify_one();
}
bool ChannelEventDistributor::add_new_distr_queue(const std::string &_chid)
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    log->debug("{} - creating distribution queue for chid '{}'",__PRETTY_FUNCTION__,_chid);
    #endif    
    auto &res = sch_dist_queues_[_chid];
    // ### it really exists in the map
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
    // ### make valid return value
    return true;
}
int ChannelEventDistributor::get_distr_server_real_bind_port(pub_svr_t *_svr,const int &_idx,const bool &_dalloc)
{
    const bool auto_port_alloc_enabled=get_config()->publisher_servers().server_auto_port_alloc_enabled();
    int port=0;
        if ( auto_port_alloc_enabled )
        {
            port=get_config()->publisher_server().bind_port()+_idx;
            #ifdef _C_DEBUG_
            log->debug("{} - actual predicted real bind port '{}'...",__PRETTY_FUNCTION__,port);
            #endif
            if (_dalloc)
            {
                #ifdef _C_DEBUG_
                log->debug("{} - actual predicted bind port '{}'... but we are checking ccq_reusable_ports_...",__PRETTY_FUNCTION__,port);
                #endif
                if ( ccq_reusable_ports_.try_pop(port) )
                {
                    #ifdef _C_DEBUG_
                    log->debug("{} - port '{}' reused from ccq_reusable_ports_...(ccq_reusable_ports_.size:'{}')",__PRETTY_FUNCTION__,port,ccq_reusable_ports_.size());
                    #endif
                }
                else
                {
                    #ifdef _C_DEBUG_
                    log->debug("{} - there was no reusable ports, calculating...",__PRETTY_FUNCTION__);
                    #endif
                    calculate_real_distr_server_bind_port(port);
                }
            }
        }
        else
        {
            port=_svr->bind_port();
        }
    return port;
}
// return value is zero if not add, the last index otherwise
int ChannelEventDistributor::add_new_pub_svr_to_config(const std::string &_chid,pub_svr_t **_svr)
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif

    pub_svrs_t *psvrs = get_config()->mutable_publisher_servers();

    // ### create some config validator function for these situations
    if ( 0==psvrs->default_ttl_multiplier_value() ) psvrs->set_default_ttl_multiplier_value(1);
    
    const int default_ttl = psvrs->dynamic_distr_server_default_ttl_sec()*psvrs->default_ttl_multiplier_value();
    const int pre_size = psvrs->server_size();
    
    const bool servers_empty = psvrs->server().empty(); // ### create logic if no servers configured - default value...!!!
    
    if ( !servers_empty )
    {
        auto last_item = psvrs->server().end()-1;
        #ifdef _C_DEBUG_
        log->debug("{} - servers_empty: '{}', last item : '{}'",__PRETTY_FUNCTION__,servers_empty,last_item->ShortDebugString());
        #endif
        const std::string bind_ip = last_item->bind_ip();
        int bind_port=0;
        if ( get_config_distr_server_free_bind_port(bind_port) )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - predicted bind port '{}'...",__PRETTY_FUNCTION__,bind_port);
            #endif
            *_svr = psvrs->add_server();
            (*_svr)->set_enabled( true );
            (*_svr)->set_chid( _chid );
            (*_svr)->set_bind_ip( bind_ip );
            (*_svr)->set_bind_port( bind_port );
            (*_svr)->set_ttl_sec( default_ttl );
            (*_svr)->set_test_mode( false );
        }
        #ifdef _C_DEBUG_
        else
        {
            log->error("{} - failed to get_config_distr_server_free_bind_port()...",__PRETTY_FUNCTION__);
        }
        #endif
    }
    else
    {
        // ### logic with default values
    }
    
    const int post_size = psvrs->server_size();
    #ifdef _C_DEBUG_
    if ( pre_size<=post_size )
    {
        log->debug("{} - pre_size was '{}', post_size was '{}' -> '{}'",__PRETTY_FUNCTION__,pre_size,post_size,(*_svr)->ShortDebugString());
    }
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
    if ( pre_size<=post_size )
    {
        #ifdef _C_DEBUG_
        log->debug("{} - return value '{}'",__PRETTY_FUNCTION__,post_size);
        #endif
        if( get_config()->auto_config_rewrite_enabled() )
        {
            update_config_file();
        }
        return post_size;
    }
    #ifdef _C_DEBUG_
    log->debug("{} - return value '0'",__PRETTY_FUNCTION__);
    #endif
    return 0;
}
int ChannelEventDistributor::remove_pub_svr_from_config(const std::string &_chid)
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    pub_svrs_t *psvrs = get_config()->mutable_publisher_servers();
    for(auto iter = psvrs->server().begin(); iter < psvrs->server().end(); iter++)
    {
        if (_chid==iter->chid())
        {
            psvrs->mutable_server()->erase(iter);
            #ifdef _C_DEBUG_
            log->debug("{} - server with chid '{}' has been removed...",__PRETTY_FUNCTION__,_chid);
            #endif
            break;
        }
    }
    // ### update only if the config has chenged- check some similar way: server presize/postsize
    if( get_config()->auto_config_rewrite_enabled() )
    {
        update_config_file();
    }
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
    // ### fix return value
    return 0;
}
bool ChannelEventDistributor::update_config_file()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    std::lock_guard<std::mutex> _(mtx_cfg_fn_update_);
    const std::string config_path = config_file_path();
    if ( config_path.empty() )
    {
        log->error("Unable to update config file: config file path is not set! (config is default initialized or set directly by a json string)");
        return false;
    }
    #ifdef _C_DEBUG_
    log->debug("{} - creating json format from protobuf config...",__PRETTY_FUNCTION__);
    #endif
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_primitive_fields = true;
    options.add_whitespace=true;
    std::string json;
    auto status = google::protobuf::util::MessageToJsonString(*get_config(), &json, options);
    #ifdef _C_DEBUG_
    log->debug("{} - saving file '{}'...",__PRETTY_FUNCTION__,config_path);
    #endif
    std::ofstream outputFile( config_path, std::ios::out | std::ios::binary | std::ios::trunc );
    if (outputFile.is_open())
    {
      outputFile << json;
      outputFile.close();
      log->info("Config file successfully updated");
      return true;
    }
    else
    {
      log->error("Failed to update config file: failed to open output file({})!",config_path);
    }
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
    return false;
}
bool ChannelEventDistributor::get_config_distr_server_free_bind_port(int &_bind_port)
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    
    if ( !get_config() ) return false;

    const int server_size = get_config()->mutable_publisher_servers()->server_size();
    if ( 0==server_size )
    {
        _bind_port=get_config()->publisher_server().bind_port()+1;
    }

    for(int i=0;i<server_size;i++)
    {
        pub_svr_t *svr = get_config()->mutable_publisher_servers()->mutable_server(i);
        int last_port=svr->bind_port();
        
        bool found_free_port=true;
        unsigned int new_port=last_port+1;
        #ifdef _C_DEBUG_
        log->debug("{} - searching new port number '{}' in the config...",__PRETTY_FUNCTION__,new_port);
        #endif
        for(int j=i+1;j<server_size;j++)
        {
            if ( new_port==get_config()->publisher_servers().server(j).bind_port() )
            {
                #ifdef _C_DEBUG_
                log->debug("{} - new_port found in the config (server chid: '{}')(break loop)...",__PRETTY_FUNCTION__,get_config()->publisher_servers().server(j).chid());
                #endif
                found_free_port=false;
                break;
            }
        }
            
        if (found_free_port)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - found new free port '{}'... returning...",__PRETTY_FUNCTION__,new_port);
            #endif
            _bind_port=new_port;
            return true;
        }
    }
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
    return true;
}
bool ChannelEventDistributor::calculate_real_distr_server_bind_port(int &_bind_port)
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif

    if ( sch_dist_queues_.empty() )
    {
        return false;
    }
    
    const int highest_port = get_highest_port();
    #ifdef _C_DEBUG_
    log->debug("{} - highest_port: '{}'",__PRETTY_FUNCTION__,highest_port);
    #endif
    int bind_port=highest_port+1;
    // ### CHECK it not EXISTS
    for (auto &item : sch_dist_queues_)
    {
        if ( bind_port==item.second.bind_port_ )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - bind_port '{}' already exists at channel dist '{}'! Incrementing...",__PRETTY_FUNCTION__,bind_port,item.first);
            #endif
            bind_port++;
        }
    }
    #ifdef _C_DEBUG_
    log->debug("{} - calculated bind_port is: '{}'",__PRETTY_FUNCTION__,bind_port);
    #endif
    _bind_port = bind_port;
    
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
    return true;
}
int ChannelEventDistributor::get_lowest_port() 
{
  auto it = std::min_element(std::begin(sch_dist_queues_), std::end(sch_dist_queues_),
                           [](const auto& l, const auto& r) { return l.second.bind_port_ < r.second.bind_port_; });
  return it->second.bind_port_;
}
int ChannelEventDistributor::get_highest_port()
{
  auto it = std::max_element(std::begin(sch_dist_queues_), std::end(sch_dist_queues_),
                           [](const auto& l, const auto& r) { return l.second.bind_port_ < r.second.bind_port_; });
  return it->second.bind_port_;
}
void ChannelEventDistributor::notify() // ### rename - notify svc change
{
    
    update_chid_distr_server_portlist();
    
    if (!notified_.load())
    {
        notified_.store(true);
    }
    cv_notify_.notify_all();
}
void ChannelEventDistributor::internal_notify_publisher_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    void *socket = zmq_socket (zmq_ctx, ZMQ_PUB);
    int i_linger=0;
    int rc = zmq_setsockopt (socket, ZMQ_LINGER, &i_linger, sizeof i_linger);
    rc = zmq_bind (socket, inproc_bind_address.c_str());
    while (running_.load())
    {
        {
            std::unique_lock<std::mutex> lk(mtx_notify_); // use this if only one thread will distribute
            cv_notify_.wait(lk,[&]{return notified_.load();});
        }
        
        int TxBytes=0;
        if ( 0<(TxBytes=zmq_send (socket, "NOTIFY", 6, 0)) )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - Message sent OK({})",__PRETTY_FUNCTION__,TxBytes);
            #endif
            fflush( stdout );
        }
        notified_.store(false);
    }
    rc = zmq_unbind (socket, inproc_bind_address.c_str());
    zmq_close(socket);
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void ChannelEventDistributor::internal_notify_subcriber_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    void *socket = zmq_socket (zmq_ctx, ZMQ_SUB);
    int i_linger=0;
    int rc = zmq_setsockopt (socket, ZMQ_LINGER, &i_linger, sizeof i_linger);
    // subscribe to everything
    rc = zmq_setsockopt (socket, ZMQ_SUBSCRIBE, "", 0);
    rc = zmq_connect (socket, inproc_bind_address.c_str());
    while (running_.load())
    {
        char buf [256];
        memset (buf,'\0',256);
        int RxBytes=0;
        if ( 0<(RxBytes=zmq_recv (socket, buf, 256, 0)) )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - Received '{}' bytes: '{}'",__PRETTY_FUNCTION__,RxBytes,buf);
            #endif
        }
        else if (0>RxBytes)
        {
            #ifdef _C_DEBUG_
            log->debug("{} - RxBytes '{}' - errno: '{}' (just a shutdown...)",__PRETTY_FUNCTION__,RxBytes,errno);
            #endif
        }
    }
    rc = zmq_disconnect (socket, inproc_bind_address.c_str());
    zmq_close(socket);
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
bool ChannelEventDistributor::serialize_pbmsg_to_codedstream(const system_msg_t &_msg, char **_ackBuf, int &_bufsize)
{
    size_t varintsize = google::protobuf::io::CodedOutputStream::VarintSize32(_msg.ByteSizeLong());
    size_t ackSize=_msg.ByteSizeLong()+varintsize;
    _bufsize = static_cast<int>(ackSize);
    *_ackBuf=new char[_bufsize];
    //write varint delimiter to buffer
    google::protobuf::io::ArrayOutputStream arrayOut(*_ackBuf, _bufsize);
    google::protobuf::io::CodedOutputStream codedOut(&arrayOut);
    codedOut.WriteVarint32(  static_cast<int>(_msg.ByteSizeLong()) );
    //write protobuf ack to buffer
    if ( _msg.SerializeToCodedStream(&codedOut) )
    {
        return true;
    }
    #ifdef _C_DEBUG_
    else
    {
        log->error("{} - Failed to serialize message to codedStream!",__PRETTY_FUNCTION__);
    }
    #endif
    return false;
}
bool ChannelEventDistributor::parse_pbmsg_from_codedstream(system_msg_t &_msg, const std::string &_data)
{
    google::protobuf::io::ArrayInputStream arrayIn(_data.c_str(), _data.size());
    google::protobuf::io::CodedInputStream codedIn(&arrayIn);
    google::protobuf::uint32 size=0;
    if ( codedIn.ReadVarint32(&size) )
    {
        if ( _msg.ParseFromCodedStream(&codedIn) && codedIn.ConsumedEntireMessage() )
        {
            return true;
        }
        #ifdef _C_DEBUG_
        else
        {
            log->error("{} - ParseFromCodedStream(&codedIn) && codedIn.ConsumedEntireMessage() failed! (varint size:'{}')",__PRETTY_FUNCTION__,size);
        }
        #endif
    }
    #ifdef _C_DEBUG_
    else
    {
        log->error("{} - codedIn.ReadVarint failed! (varint size:'{}'",__PRETTY_FUNCTION__,size);
    }
    #endif
    return false;
}
void ChannelEventDistributor::update_chid_distr_server_portlist()
{
    std::lock_guard<std::mutex> _(mtx_portlist_);
    
    chid_distr_server_portlist_.clear();
    // collect channels and related ports
    for (auto &item : sch_dist_queues_)
    {
        const std::string chid = item.first;
        const int bind_port = item.second.bind_port_;
        chid_distr_server_portlist_[chid]=std::to_string(bind_port);        
    }
}
std::map<std::string,std::string> ChannelEventDistributor::get_chid_distr_server_portlist() const
{
    std::lock_guard<std::mutex> _(mtx_portlist_);
    return chid_distr_server_portlist_;    
}
bool ChannelEventDistributor::create_port_svc_msg(system_msg_t &_msg)
{
    const std::string port_svc_address = get_config()->port_service().port_svc_address();
    psvc_portlist_msg_t *portlist = _msg.mutable_port_svc_msg()->mutable_portlist();
    portlist->set_port_svc_address( port_svc_address );
    auto portlist_map = get_chid_distr_server_portlist();
    auto& map = *portlist->mutable_map();
    map = google::protobuf::Map<std::string, std::string>( portlist_map.begin(),portlist_map.end() );
    return true; // ### fix return value
}
