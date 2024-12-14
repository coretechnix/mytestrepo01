#include "aveco-gpiserver.h"
#include <iostream>
#include <memory>
#include <getopt.h>

std::unique_ptr<AvecoGPIServer> svr;

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

struct cli_param_t {
        bool has_cli_param_value = false;
        std::string config_path;
};

void set_signal_handler();
void parse_cli_params(int argc,char **argv,struct cli_param_t &_cli_params);

int main(int argc, char **argv)
{
    set_signal_handler();
    std::string primary_config_path="cedm-avecogpi.conf";
    const std::string secondary_config_path = "/etc/subsys/cedm-avecogpi.conf";
    
    cli_param_t cli_params;
    if (1<argc)    
    {
        parse_cli_params(argc,argv,cli_params);
    }
    if ( cli_params.has_cli_param_value )
    {
        primary_config_path=cli_params.config_path;
    }

    svr = std::make_unique<AvecoGPIServer>();
    if (!svr->read_config_file(primary_config_path))
    {
        if(!svr->read_config_file(secondary_config_path))
        {
            printf("ERROR - Configuration file not found!\n");
            return EXIT_FAILURE;
        }
    }

    if (svr->init())
    {
        svr->run();
    }
    else
    {
        printf("ERROR - Initialization failed!\n");
        return EXIT_FAILURE;
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
void parse_cli_params(int argc,char **argv,struct cli_param_t &_cli_params)
{
    while (1)
    {
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
                    _cli_params.config_path.assign(optarg);
                    if ( !_cli_params.has_cli_param_value ) _cli_params.has_cli_param_value=true;
                    break;
            default:
                    printf("?? getopt returned character code 0%o ??\n", c);
        }
    }
}
