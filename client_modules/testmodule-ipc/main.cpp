// Client side C/C++ program to demonstrate
#include <iostream>
#include <iomanip>
#include <random>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <stdio.h>
#include <sys/socket.h>
//#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/un.h>
#include <sys/types.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/util/json_util.h>
#include "System.Comm.pb.h"

//static const char* ipc_path = "/tmp/ipc0.socket";
#define BUFF_SIZE 10485760 // 10 MB

typedef System::Comm::Message msg_t;
typedef System::Comm::Event::Distr::ChannelEvent ch_evt_msg_t;
typedef System::Comm::Event::Distr::ch_evt_t ch_evt_t;

std::string timestamp();
int main(int argc, char **argv)
{
    int c;
    int digit_optind = 0;

    std::string ipc_path;
    std::string chid;
    std::string type;
    std::string media_item;
    ch_evt_t evt=ch_evt_t::INVALID_EVENT;
    
    while (1)
    {
        // ./evtdistr_test_module_ipc.bin -i/tmp/ipc1.socket -cM2 -tstart -mMEDIA-001
        // ./evtdistr_test_module_ipc.bin --ipc_path=/tmp/ipc1.socket --chid=M2 -type==start -media=MEDIA-001
        
        int this_option_optind = optind ? optind : 1;
        int option_index = 0;
        static struct option long_options[] = {
             {"ipc_path",    required_argument, 0,  'i' },
             {"chid",    required_argument, 0,  'c' },
             {"type",    required_argument, 0,  't' },
             {"media",   required_argument, 0,  'm' },
             {0,         0,                 0,  0 }
        };

        c = getopt_long(argc, argv,"i:c:t:m:",long_options,&option_index);
        if (c == -1)
            break;

        switch (c)
        {
            case 0:
                    //printf("option %s", long_options[option_index].name);
                    //if (optarg) printf(" with arg '%s'\n", optarg);
                    break;
            case 'i':
                    //printf("option h (host):'%s'\n",optarg);
                    ipc_path.assign( std::string(optarg) );
                    break;
            case 'c':
                    //printf("option c (chid):'%s'\n",optarg);
                    chid.assign( optarg );
                    break;
            case 't':
                    //printf("option c (chid):'%s'\n",optarg);
                    type.assign( optarg );
                    break;
            case 'm':
                    //printf("option m (media_item):'%s'\n",optarg);
                    media_item.assign( optarg );
                    break;
            default:
                    printf("?? getopt returned character code 0%o ??\n", c);
        }

    }

    if (ipc_path.empty())
    {
        ipc_path = "/tmp/ipc1.socket";
    }

    if (chid.empty())
    {
        chid = "M2";
    }

    if (type.empty())
    {
        type="TEST";
        evt=ch_evt_t::TEST;
    }
    else
    {
        std::transform(type.begin(), type.end(), type.begin(), ::tolower);
        if ("start"==type)
        {
            evt=ch_evt_t::START;
        }
        if ("stop"==type)
        {
            evt=ch_evt_t::STOP;
        }
        if ("test"==type)
        {
            evt=ch_evt_t::TEST;
        }

    }

    if (media_item.empty())
    {
        media_item = "#TEST-MEDIA-ITEM-NAME#";
    }
    
    printf("Connecting to '%s', Message gen. params: chid: '%s', event type: '%s', media item: '%s'\n",ipc_path.c_str(),chid.c_str(),type.c_str(),media_item.c_str());

	int sock = 0, valread;
	struct sockaddr_un serv_addr;
	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
	{
		printf("\n Socket creation error \n");
		exit(EXIT_FAILURE);
	}
	
	serv_addr.sun_family = AF_UNIX;
        strcpy( serv_addr.sun_path, ipc_path.c_str() );
        
	if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	{
		printf("\nConnection Failed \n");
		exit(EXIT_FAILURE);
	}
	
        msg_t msg;
        msg.mutable_channel_event()->set_chid( chid );
        msg.mutable_channel_event()->set_type( evt );
        msg.mutable_channel_event()->set_media_item( media_item );
        msg.mutable_channel_event()->set_ts( timestamp() );
        
	printf("Sending message: '%s'\n",msg.ShortDebugString().c_str());
	
	// =================================================================
	// === SERIALIZATION TEST - SERIALIZE TO CODED STREAM ==============
	// =================================================================
	size_t varintsize = google::protobuf::io::CodedOutputStream::VarintSize32(msg.ByteSizeLong());
	size_t ackSize=msg.ByteSizeLong()+varintsize;
	const int bufsize = static_cast<int>(ackSize);
	char* ackBuf=new char[bufsize];
	//write varint delimiter to buffer
	google::protobuf::io::ArrayOutputStream arrayOut(ackBuf, bufsize);
	google::protobuf::io::CodedOutputStream codedOut(&arrayOut);
	codedOut.WriteVarint32(  static_cast<int>(msg.ByteSizeLong()) );
	//write protobuf ack to buffer
	if ( msg.SerializeToCodedStream(&codedOut) )
	{
	    printf("Message successfully serialized to codedStream\n");
	    printf("encapsulated protobuf msg size: '%d'\n",bufsize);
	    // Now it is possible to send over the wire : rc = send (socket, ackBuf, bufsize, 0);
	    //int TxBytes = send(sock, msg_str.data(), msg_str.length(), 0);
	    int TxBytes = send(sock, ackBuf, bufsize, 0);
	    printf("Number of bytes sent: '%d'\n",TxBytes);
        }
        else
        {
	    printf("ERROR - Failed to serialize message to codedStream!\n");
        }
    	delete [] ackBuf;
	return 0;
}
std::string timestamp()
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
