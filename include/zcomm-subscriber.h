#pragma once
#include <string>
#include <vector>
#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

#ifndef SUCCESS
#define SUCCESS 0
#endif

#ifndef FAILED
#define FAILED (-1)
#endif

#ifndef INFINITE
#define INFINITE 0xFFFFFFFF
#endif

#ifndef NULL
#define NULL 0
#endif

typedef signed short int SHORT;
typedef unsigned short int USHORT;
typedef void* HANDLE;
typedef unsigned char BYTE;
typedef unsigned char UBYTE;
typedef void* LPVOID;

#if defined(__linux__)
typedef long long LONGLONG;
typedef int BOOL;
typedef BOOL BOOL32;
typedef unsigned short int WORD;
typedef uint16_t UWORD;
typedef int32_t LONG;
typedef uint32_t ULONG;
typedef unsigned long long ULONGLONG;
#elif defined(__APPLE__)
#include <libkern/OSTypes.h>
typedef SInt64 LONGLONG;
typedef SInt32 BOOL32;
typedef UInt16 WORD;
typedef UInt16 UWORD;
typedef SInt32 LONG;
typedef UInt32 ULONG;
typedef UInt64 ULONGLONG;
#else
typedef __int64 LONGLONG;
typedef int BOOL;
typedef BOOL BOOL32;
typedef unsigned short int WORD;
typedef long LONG;
typedef unsigned long ULONG;
typedef unsigned long long ULONGLONG;
#endif

namespace zcomm {

    class Subscriber
    {
    	public:
    		typedef std::string transport_t; //tcp, ipc, inproc, pgm, epgm, vmci
    		typedef std::string address_t;
		typedef std::string port_t;
		
		typedef struct {
                	transport_t transport_type;
                	address_t address;
                	port_t port;
               } pub_endpoint_t;

		Subscriber() = default;
		~Subscriber() = default;
		int setsockopt(const int _option_name, const void *_option_value, const size_t _option_len);
		void add_publisher_endpoint(const pub_endpoint_t _pub_endpoint);
		BOOL32 init();
		int subscribe(const std::string &_filter="");
		int set_recvtimeo(const int &_timeo=-1); // -1: infinite, 0 return immediately, with a EAGAIN error, 0< it will wait for a message for that amount of time before returning with an EAGAIN error
		int connect(); // return value: number of successfully connected peers
		int recv(void *buf, size_t len, int flags=0);
		int recv(std::string &_str, int flags=0);
		int disconnect(); // return value: number of successfully disconnected peers
		int reconnect();
		int close();
		BOOL32 terminate();
	private:
		void *context=NULL;
		void *socket=NULL;
		
		std::vector<std::string> connections;
		
		bool shutdown_called = false;
		bool binded = false;
		bool ok = true;
    };

}
