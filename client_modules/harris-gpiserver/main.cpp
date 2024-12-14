#include "harris-gpiserver.h"
#include <iostream>
#include <memory>
#include <getopt.h>
std::unique_ptr<HarrisGPIServer> svr;

#if defined(_MSC_VER)
#include <cstdio>
#include <windows.h>
#include <stdio.h>
BOOL WINAPI handler_func(DWORD signal)
{
        if (signal == CTRL_C_EVENT)
        {
                svr->stop();
        }
        return TRUE;
}
#elif defined(__GNUC__)
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
void handler_func(int s)
{
        svr->stop();
}
#endif

void set_signal_handler();

int main(int argc, char **argv)
{
    set_signal_handler();

    std::string cliparm_config_path;
    while (1)
    {
        //int this_option_optind = optind ? optind : 1;
        int option_index = 0;
        static struct option long_options[] = {
             {"config",  required_argument, 0,  'c'},
             {0,         0,                 0,  0 }
        };

        int c = getopt_long(argc, argv,"c:",long_options,&option_index);
        if (c == -1)
            break;

        switch (c)
        {
            case 0:
                    printf("option %s", long_options[option_index].name);
                    if (optarg) printf(" with arg '%s'\n", optarg);
                    break;
            case 'c':
                    cliparm_config_path.assign(optarg);
                    break;
            default:
                    printf("?? getopt returned character code 0%o ??\n", c);
        }
    }

    svr = std::make_unique<HarrisGPIServer>();

    //const std::string path = "cedm-harrisgpi.conf";
    const std::string path = ( !cliparm_config_path.empty() ? cliparm_config_path : "cedm-harrisgpi.conf");
    const std::string etcpath = "/etc/subsys/cedm-harrisgpi.conf";

    if (!svr->read_config_file(path))
    {
        if(!svr->read_config_file(etcpath))
        {
            printf("ERROR - Configuration file not found!\n");
            return EXIT_FAILURE;
        }
    }

    if (svr->init())
    {
        svr->run();
    }

    return EXIT_SUCCESS;
}

void set_signal_handler()
{
    #if defined(_MSC_VER)
        if (!SetConsoleCtrlHandler(handler_func, TRUE)) {
                printf("\nERROR: Could not set control handler");
                return 1;
        }
    #elif defined(__GNUC__)
        struct sigaction sigIntHandler;
        sigIntHandler.sa_handler = handler_func;
        sigemptyset(&sigIntHandler.sa_mask);
        sigIntHandler.sa_flags = 0;
        sigaction(SIGINT, &sigIntHandler, NULL);
    #endif

    signal(SIGPIPE, SIG_IGN);
}
