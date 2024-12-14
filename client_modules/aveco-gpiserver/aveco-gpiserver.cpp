#include "aveco-gpiserver.h"
#include "System.Config.Event.Distr.Server.Modul.Aveco.GPI.pb.h"
#include "System.Comm.pb.h"
#include "netcomm-client.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/util/json_util.h>

#include <cassert>
#include <zmq.h>
#include <algorithm>
#include <future>         // std::async, std::future
#include <unordered_map>
#include <iomanip>
#include <fstream>
#include <system_error>
#include <arpa/inet.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/null_sink.h"
#include <spdlog/spdlog.h>

// Universally unique identifier
#include <uuid/uuid.h>

#define RECV_BUFF_SIZE 256
typedef System::Comm::Event::Distr::ChannelEvent ch_evt_msg_t;

AvecoGPIServer::AvecoGPIServer()
{}
AvecoGPIServer::~AvecoGPIServer()
{
    google::protobuf::ShutdownProtobufLibrary();
}
void AvecoGPIServer::set_logger_uuid(const std::string &_value)
{
    logger_uuid_=_value;
}
std::string AvecoGPIServer::logger_uuid() const
{
    return logger_uuid_;
}
void AvecoGPIServer::create_logger()
{

    const std::string object_name = "harris-gpi";
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
        }
        else
        {
            log = spdlog::create<spdlog::sinks::null_sink_st>("null_logger");
        }
    }
    catch (const spdlog::spdlog_ex& ex)
    {
        printf("AvecoGPIServer - ERROR - logger initialization failed! Trying to create console logger...\n");
        try {
            log = spdlog::stdout_color_mt("console");
        }
        catch (const spdlog::spdlog_ex& ex)
        {
            printf("AvecoGPIServer - ERROR - console logger initialization failed! Trying to create null logger...\n");
            try {
                log = spdlog::create<spdlog::sinks::null_sink_st>("null_logger");
            }
            catch (const spdlog::spdlog_ex& ex)
            {
                printf("AvecoGPIServer - ERROR - null logger initialization failed!\n");
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
const std::shared_ptr<config_t>& AvecoGPIServer::get_config() const
{
  std::shared_lock<std::shared_mutex> _(mtx_config);
  return configPtr;
}
std::shared_ptr<config_t>& AvecoGPIServer::get_config()
{
  std::lock_guard<std::shared_mutex> _(mtx_config);
  return configPtr;
}
bool AvecoGPIServer::set_config(const std::string &_json_str)
{
    if ( get_config() ) get_config().reset();
    get_config() = std::make_shared<config_t> ();
    google::protobuf::util::JsonParseOptions options;
    options.ignore_unknown_fields=true;
    auto status = google::protobuf::util::JsonStringToMessage(_json_str, get_config().get(), options);
    if ( !status.ok() )
    {
        printf("AvecoGPIServer - ERROR - Failed to parse configuration data (json format)!\n");
    }
    return status.ok();
}
bool AvecoGPIServer::read_config_file(const std::string &_config_file_path)
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
            //log()->debug( "{} - JsonStringToMessage is Ok returning",__PRETTY_FUNCTION__ );
            #endif
            return true;
        }
        else
        {
            //log()->error( "{} - Failed to JsonStringToMessage()",__PRETTY_FUNCTION__ );
        }
    }
    else
    {
        //log()->error( "{} - Failed to read config file",__PRETTY_FUNCTION__ );
    }

    #ifdef _C_DEBUG_
    //log()->debug( "{} - Something went wrong! Return NULL...",__PRETTY_FUNCTION__ );
    #endif

    return false;
}
void AvecoGPIServer::set_running(const bool &_value)
{
    running_.store(_value);
}
bool AvecoGPIServer::running()
{
    return running_.load();
}
void AvecoGPIServer::set_master_connected(const bool &_value)
{
    master_connected_.store(_value);
}
bool AvecoGPIServer::master_connected()
{
    return master_connected_.load();
}
std::vector<std::thread>& AvecoGPIServer::get_threads()
{
    std::lock_guard<std::mutex> _(mtx_threads_);
    return threads_;
}
bool AvecoGPIServer::init_trigger_mapping_table()
{
    if (!get_config())
    {
        return false;
    }
    
    log->info("initializing trigger mapping table...");
    ch_evt_msg_map_ = std::unordered_map<std::string, ch_evt_msg_t> ( std::unordered_map<std::string, ch_evt_msg_t>(get_config()->ch_evts().map().begin(), get_config()->ch_evts().map().end()) );
    for (std::pair<std::string, ch_evt_msg_t> element : ch_evt_msg_map_)
    {
        log->info("{} :: {}",element.first,element.second.ShortDebugString());
    }
    
    if ( ch_evt_msg_map_.empty() )
    {
        return false;
    }
    
    return true;
}
bool AvecoGPIServer::init()
{
    create_logger();

    if ( !init_trigger_mapping_table() )
    {
        log->info("failed to initialize trigger mapping table");
        return false;
    }
    
    bool master_server_enabled = get_config()->gpiserver().master().enabled();
    bool backup_server_enabled = get_config()->gpiserver().master().enabled();
    if ( !master_server_enabled && !backup_server_enabled )
    {
        log->info("none of the target servers are enabled!");
        return false;
    }
    return true;
}
void AvecoGPIServer::run()
{
    t_data_sender_comm_thread = std::thread(&AvecoGPIServer::data_sender_comm_thread,this);

    bool master_server_enabled = get_config()->gpiserver().master().enabled();
    bool backup_server_enabled = get_config()->gpiserver().master().enabled();
    
    if ( master_server_enabled )
    {
        get_threads().push_back( std::thread (&AvecoGPIServer::master_gpi_server_comm_thread, this) );
    }

    if ( backup_server_enabled )
    {
        get_threads().push_back( std::thread (&AvecoGPIServer::backup_gpi_server_comm_thread, this) );
    }

    if ( !get_threads().empty() )
    {
        #ifdef _C_DEBUG_
        log->debug("waiting for threads...");
        #endif
        for (std::thread &th : get_threads())
        {
            if (th.joinable()) th.join();
        }
    }
    
    notify_data_sender_comm_thread();
    
    t_data_sender_comm_thread.join();

    if (log)
    {
        spdlog::drop( logger_uuid() );
        spdlog::shutdown();
    }
}
void AvecoGPIServer::stop()
{
    set_running( false );
}
void AvecoGPIServer::notify_data_sender_comm_thread()
{
    data_comm_sender_cv.notify_one();
}
void AvecoGPIServer::data_sender_comm_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    std::unique_ptr<NetCommClient> client;
    const bool ipc_enabled = get_config()->module_comm_server().ipc_enabled();
    const std::string ipc_bind_address = get_config()->module_comm_server().ipc_bind_address();
    const bool tcp_enabled = get_config()->module_comm_server().tcp_enabled();
    bool is_comm_channel = true;
    int sock = 0, valread;
    
    if ( ipc_enabled )
    {
        #ifdef _C_DEBUG_
        log->debug("{} - ipc comm enabled, setting up connection...",__PRETTY_FUNCTION__);
        #endif
        if ( !ipc_bind_address.empty() )
        {    
            client = std::make_unique<IpcClient>(ipc_bind_address);
            client->SetCheckInterval( std::chrono::milliseconds(100) );
            client->SetRecvTimeoutSec(1);
            if ( !client->Connect() )
            {
                client->AsyncConnect();
            }
        }
        else
        {
            #ifdef _C_DEBUG_
            log->debug("{} - data_sender_comm_thread - ipc bind address is empty!",__PRETTY_FUNCTION__);
            #endif
            is_comm_channel=false;
        }
    }
    else
    {
        #ifdef _C_DEBUG_
        log->debug("{} - ipc comm is disabled, trying tcp...",__PRETTY_FUNCTION__);
        #endif
        
        if ( tcp_enabled )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - tcp comm enabled, setting up connection...",__PRETTY_FUNCTION__);
            #endif
            const std::string bind_ip = get_config()->module_comm_server().tcp_bind_ip();
            const int bind_port = get_config()->module_comm_server().tcp_bind_port();
            client = std::make_unique<TcpClient>(bind_ip, bind_port);
            client->SetCheckInterval( std::chrono::milliseconds(100) );
            client->SetRecvTimeoutSec(1);
            if ( !client->Connect() )
            {
                client->AsyncConnect();
            }
        }
        else
        {
            #ifdef _C_DEBUG_
            log->debug("{} - tcp comm is disabled",__PRETTY_FUNCTION__);
            #endif
        }
    }
    
    #ifdef _C_DEBUG_
    log->debug("{} - comm channel is configured, starting to process...",__PRETTY_FUNCTION__);
    #endif
        
    while( running() )
    {
            
        std::unique_lock<std::mutex> lk(redis_pub_mtx);
        
        auto now = std::chrono::system_clock::now();
        if( data_comm_sender_cv.wait_until(lk, now + std::chrono::seconds(1), [this](){ return !msg_broadcast_list.empty(); }) )
        {
            lk.unlock();
            if ( running() )
            {
                client->Send(msg_broadcast_list.front().data(), msg_broadcast_list.front().size());
                msg_broadcast_list.pop();
            }
            #ifdef _C_DEBUG_
            else
            {
                log->debug( "{} - shutdown event! Notification skipped!",__PRETTY_FUNCTION__ );
            }
            #endif
        }
        else // timeout
        {
            // we call the non-blocking recv to detect if the connection was broken and start to reconnect
            std::string data = client->NbRecv();
        }
    }
    
    client.reset();
        
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void AvecoGPIServer::master_gpi_server_comm_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    std::unique_ptr<NetCommClient> client;
    const std::string server_ip = get_config()->gpiserver().master().ip();
    const int server_port = get_config()->gpiserver().master().port();
    client = std::make_unique<TcpClient>(server_ip, server_port);
    client->SetCheckInterval( std::chrono::milliseconds(500) );
    client->SetRecvTimeoutSec(1);
    if ( !client->Connect() )
    {
        client->AsyncConnect();
    }
    
    while( running() )
    {
        std::string data = client->Recv();
        if (!data.empty())
        {
            #ifdef _C_DEBUG_
            log->debug("{} - data received: '{}'",__PRETTY_FUNCTION__,data);
            #endif
            for(int i=0;i<data.size();i++)
            {
                const std::string event_char (1,data[i]);
                std::unordered_map<std::string,ch_evt_msg_t>::const_iterator got = ch_evt_msg_map_.find (event_char);
                if ( got != ch_evt_msg_map_.end() )
                {
                    system_msg_t msg;
                    msg.mutable_channel_event()->CopyFrom(got->second);
                    msg.mutable_channel_event()->set_ts( timestamp() );
                    #ifdef _C_DEBUG_
                    log->debug("{} - full message: '{}'",__PRETTY_FUNCTION__,msg.ShortDebugString());
                    #endif
                    // === SEND ====================================================================================================================
                    char* ackBuf;
                    int bufsize;
                    if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                    {
                        #ifdef _C_DEBUG_
                        log->debug("{} - message successfully serialized to codedStream",__PRETTY_FUNCTION__);
                        log->debug("{} - encapsulated protobuf msg size: '{}'",__PRETTY_FUNCTION__,bufsize);
                        #endif
                        
                        std::string msgstr(ackBuf, bufsize);
                        msg_broadcast_list.push( msgstr );
                        notify_data_sender_comm_thread();
                        
                        #ifdef _C_DEBUG_
                        log->debug("{} - publishing message: '{}'",__PRETTY_FUNCTION__,got->second.ShortDebugString());
                        #else
                        log->debug("[master] publishing message: '{}'",got->second.ShortDebugString());
                        #endif
                    }
                    delete [] ackBuf;
                    // =============================================================================================================================
                }
                else
                {
                    if (get_config()->debug()) log->warn("[master] Event('{}'): Not found in the channel event message map",event_char);
                }
            }
        }
        
        // ### FIX THIS PART
        if ( !client->IsConnected() )
        {
            if ( master_connected() )
            {
                set_master_connected(false);
            }
            log->warn("[master] waiting for connected state...");
            if ( client->WaitForConnected() )
            {
                log->info("[master] connected, setting master to OK again...");
                set_master_connected();
            }
        }
        #ifdef _C_DEBUG_
        log->debug("{} - master connection state: '{}'",__PRETTY_FUNCTION__,client->IsConnected());
        #endif
    }

    //if ( client->IsConnected() ) 
    //{
        client.reset();
    //}
    
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
void AvecoGPIServer::backup_gpi_server_comm_thread()
{
    #ifdef _C_DEBUG_
    log->debug("{} - started",__PRETTY_FUNCTION__);
    #endif
    std::unique_ptr<NetCommClient> client;
    const std::string server_ip = get_config()->gpiserver().backup().ip();
    const int server_port = get_config()->gpiserver().backup().port();
    client = std::make_unique<TcpClient>(server_ip, server_port);
    client->SetCheckInterval( std::chrono::milliseconds(500) );
    client->SetRecvTimeoutSec(1);
    #ifdef _C_DEBUG_
    log->debug("{} - Connecting...",__PRETTY_FUNCTION__);
    #endif
    if ( !client->Connect() )
    {
        client->AsyncConnect();
    }

    while( running() )
    {
        #ifdef _C_DEBUG_
        log->debug("{} - Main loop is running...",__PRETTY_FUNCTION__);
        #endif
        std::string data = client->Recv();
        if (!data.empty())
        {
            #ifdef _C_DEBUG_
            log->debug("{} - data received: '{}'",__PRETTY_FUNCTION__,data);
            #endif
            for(int i=0;i<data.size();i++)
            {
                const std::string event_char (1,data[i]);
                std::unordered_map<std::string,ch_evt_msg_t>::const_iterator got = ch_evt_msg_map_.find (event_char);
                if ( got != ch_evt_msg_map_.end() )
                {
                    system_msg_t msg;
                    msg.mutable_channel_event()->CopyFrom(got->second);
                    msg.mutable_channel_event()->set_ts( timestamp() );
                    #ifdef _C_DEBUG_
                    printf("{} - full message: '%s'\n",__PRETTY_FUNCTION__,msg.ShortDebugString().c_str());
                    #endif
                    // === SEND ====================================================================================================================
                    if ( !master_connected() )
                    {
                        char* ackBuf;
                        int bufsize;
                        if (serialize_pbmsg_to_codedstream(msg,&ackBuf,bufsize))
                        {
                            #ifdef _C_DEBUG_
                            log->debug("{} - message successfully serialized to codedStream",__PRETTY_FUNCTION__);
                            log->debug("{} - encapsulated protobuf msg size: '{}'",__PRETTY_FUNCTION__,bufsize);
                            #endif
                            
                            std::string msgstr(ackBuf, bufsize);
                            msg_broadcast_list.push( msgstr );
                            notify_data_sender_comm_thread();
                            
                            log->warn("[backup] master is not connected, sending message from backup...");
                            
                            #ifdef _C_DEBUG_
                            if (get_config()->debug()) log->debug("{} - prepare message for publishing: '{}'",__PRETTY_FUNCTION__,got->second.ShortDebugString());
                            #else
                            log->debug("[backup] prepare message for publishing: '{}'",got->second.ShortDebugString());
                            #endif
                        }
                        delete [] ackBuf;
                    
                    }
                    #ifdef _C_DEBUG_
                    else
                    {
                        log->warn("[backup] master is connected, not sending...");
                    }
                    #endif
                    // =============================================================================================================================
                }
                else
                {
                    if (get_config()->debug()) log->warn("Event('{}'): Not found in the channel event message map",event_char);
                }
            }
        }
        if ( !client->IsConnected() )
        {
            #ifdef _C_DEBUG_
            log->debug("{} - WaitForConnected...",__PRETTY_FUNCTION__);
            #endif
            client->WaitForConnected();
        }
        #ifdef _C_DEBUG_
        log->debug("backup connection state: '{}'",client->IsConnected());
        #endif
    }
    
    //if ( client->IsConnected() )
    //{
        client.reset();
    //}
    
    #ifdef _C_DEBUG_
    log->debug("{} - done",__PRETTY_FUNCTION__);
    #endif
}
bool AvecoGPIServer::read_file(const std::string &_file_path, std::string &_data, std::string &_errm, int &_errc)
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
std::string AvecoGPIServer::timestamp()
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
bool AvecoGPIServer::serialize_pbmsg_to_codedstream(const system_msg_t &_msg, char **_ackBuf, int &_bufsize)
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
bool AvecoGPIServer::parse_pbmsg_from_codedstream(system_msg_t &_msg, const std::string &_data)
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
        log->error("{} - codedIn.ReadVarint failed! (varint size:'{}')",__PRETTY_FUNCTION__,size);
    }
    #endif
    return false;
}
bool AvecoGPIServer::parse_pbmsg_from_codedstream(system_msg_t &_msg, const char *_ackBuf, const int &_bufsize)
{
    // ###
    return true;
}
