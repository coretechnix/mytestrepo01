#include <iostream>
#include <string>
#include <atomic>
#include <chrono>
#include <thread>
#include <cstring>
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

class TcpClient {

public:

    TcpClient(const std::string& serverIP, int serverPort);
    ~TcpClient();
    virtual bool Connect();
    virtual void AsyncConnect();
    virtual void Disconnect();
    bool Send(const std::string& message);
    bool Send(const char *_buf, const int _bufsize);
    bool NbSend(const std::string& message);
    std::string Recv();
    std::string NbRecv();
    bool IsConnected() const;
    void SetCheckInterval(const std::chrono::seconds& interval);
    void SetSocketOptions();
    bool SetRecvTimeoutSec(const int &_sec=1);
protected:
    std::string serverIP_;
    int serverPort_;
    int clientSocket_;
    std::atomic<bool> connected_;
    std::atomic<bool> stopRequested_;
    std::chrono::seconds checkInterval_;
    std::thread th_conn_check_;
    
    bool Initialize();
    virtual bool CreateSocket();
    virtual bool ConnectToServer();
    void CloseSocket();
    void Cleanup();
    int GetLastErrorCode();
    void CheckConnection();
private:
};

class IpcClient : public TcpClient {

public:

    IpcClient(const std::string &_bind_address);
    //~IpcClient();
    bool Connect();
    void AsyncConnect();
    void Disconnect();
    //bool Send(const std::string& message);
    //bool Send(const char *_buf, const int _bufsize);
    //bool NbSend(const std::string& message);
    //std::string Recv();
    //std::string NbRecv();
    //bool IsConnected() const;
    //void SetCheckInterval(const std::chrono::seconds& interval);
    //void SetSocketOptions();
    //bool SetRecvTimeoutSec(const int &_sec=1);
private:
    std::string ipc_bind_address_;
    //std::string serverIP_;
    //int serverPort_;
    //int clientSocket_;
    //std::atomic<bool> connected_;
    //std::atomic<bool> stopRequested_;
    //std::chrono::seconds checkInterval_;
    //std::thread th_conn_check_;
    
    //bool Initialize();
    bool CreateSocket();
    bool ConnectToServer();
    //void CloseSocket();
    //void Cleanup();
    //int GetLastErrorCode();
    //void CheckConnection();
};
