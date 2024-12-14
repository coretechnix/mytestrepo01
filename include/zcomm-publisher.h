#pragma once
#include <string>
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

    class Publisher
    {
    	public:
                typedef std::string transport_t; //tcp, ipc, inproc, pgm, epgm, vmci
                typedef std::string address_t;
                typedef std::string port_t;
                    	
		Publisher() = default;
		~Publisher() = default;
		int setsockopt (const int _option_name, const void *_option_value, const size_t _option_len);
		BOOL32 init();
		BOOL32 bind();
		int  send(void *buf, size_t len, int flags=0);
		int  send(const std::string &_str, int flags=0);
		BOOL32 unbind();
		BOOL32 terminate();
		
		void set_transport_type(const std::string &_value);
		std::string transport_type() const;
		void set_address(const std::string &_value);
		std::string address() const;
                void set_port(const std::string &_value);
                std::string port() const;
        private:
                void *context;
                void *socket;
                
                transport_t transport_type_;
                address_t   address_;
                port_t	port_;
                bool shutdown_called = false;
                bool binded = false;
                bool ok = true;
                std::string connection_string() const;
    };

}
