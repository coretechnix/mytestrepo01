#include <iostream>
#include <string>
#include <atomic>
#include <chrono>
#include <thread>
#include <cstring>
#include <condition_variable>
#ifdef _WIN32
    #include <winsock2.h>
    #pragma comment(lib, "ws2_32.lib")
#else
    #include <sys/socket.h>
    #include <arpa/inet.h>
    #include <netinet/in.h>	/* IPPROTO_TCP, etc. */
    #include <sys/un.h>
    #include <netinet/tcp.h>    /* TCP_KEEPIDLE, etc. */
    #include <unistd.h>
#endif

class NetCommClient {
    public:
        NetCommClient();
        virtual ~NetCommClient();
        bool Connect();
        void AsyncConnect();
        void Disconnect();
        bool Send(const std::string& message);
	bool Send(const char *_buf, const int _bufsize);
        bool NbSend(const std::string& message); 
        // ### create bool NbSend(const char *_buf, const int _bufsize);
        std::string Recv();
        std::string NbRecv();
        //void SetCheckInterval(const std::chrono::seconds& interval);
        void SetCheckInterval(const std::chrono::milliseconds& interval);
        void SetSocketOptions(); // ### ez csak privatben kell - nem publikus !!!
        void SetRecvTimeoutSec(const int &_sec=1);
        // ### create SetSendTimeoutSec
        bool IsConnected() const;
        //void WaitForConnected();
        bool WaitForConnected(); // ### ezt lehetne parameterezni, hogy mi legyen a timeout sec-ben
    protected:
        int clientSocket_;
        std::atomic<bool> connected_;
        std::atomic<bool> stopRequested_;
        int recv_timeo_=0; //The default for this option is zero, which indicates that a receive operation shall not time out.
        std::chrono::milliseconds checkInterval_;
        std::thread th_conn_check_;
        
        std::mutex mtx_connected_;
        std::condition_variable cv_connected_;
        void notify_is_connected();

        virtual bool Initialize()=0;
        virtual bool CreateSocket()=0;
        virtual bool ConnectToServer()=0;
        
        void CloseSocket();
        void Cleanup();
        int GetLastErrorCode();
        void CheckConnection();
};
class TcpClient : public NetCommClient {

public:

    TcpClient(const std::string& serverIP, int serverPort);
    virtual ~TcpClient();
    //virtual bool Connect();
    //virtual void AsyncConnect();
    //virtual void Disconnect();
    //bool Send(const std::string& message);
    //bool Send(const char *_buf, const int _bufsize);
    //bool NbSend(const std::string& message);
    //std::string Recv();
    //std::string NbRecv();
    //bool IsConnected() const;
    //void SetCheckInterval(const std::chrono::seconds& interval);
    //void SetSocketOptions();
    //bool SetRecvTimeoutSec(const int &_sec=1);
protected:
    std::string serverIP_;
    int serverPort_;
    //int clientSocket_;
    //std::atomic<bool> connected_;
    //std::atomic<bool> stopRequested_;
    //std::chrono::seconds checkInterval_;
    //std::thread th_conn_check_;
    
    bool Initialize();
    //virtual bool CreateSocket();
    //virtual bool ConnectToServer();
    bool CreateSocket();
    bool ConnectToServer();
    //void CloseSocket();
    //void Cleanup();
    //int GetLastErrorCode();
    //void CheckConnection();
private:
};

//class IpcClient : public TcpClient {
class IpcClient : public NetCommClient {

public:

    IpcClient(const std::string &_bind_address);
    virtual ~IpcClient();
    /*bool Connect();
    void AsyncConnect();
    void Disconnect();*/
    //bool Send(const std::string& message);
    //bool Send(const char *_buf, const int _bufsize);
    //bool NbSend(const std::string& message);
    //std::string Recv();
    //std::string NbRecv();
    //bool IsConnected() const;
    //void SetCheckInterval(const std::chrono::seconds& interval);
    //void SetSocketOptions();
    //bool SetRecvTimeoutSec(const int &_sec=1);
//private:
protected:
    std::string ipc_bind_address_;
    //std::string serverIP_;
    //int serverPort_;
    //int clientSocket_;
    //std::atomic<bool> connected_;
    //std::atomic<bool> stopRequested_;
    //std::chrono::seconds checkInterval_;
    //std::thread th_conn_check_;
    
    bool Initialize();
    bool CreateSocket();
    bool ConnectToServer();
    //void CloseSocket();
    //void Cleanup();
    //int GetLastErrorCode();
    //void CheckConnection();
};
