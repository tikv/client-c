#pragma once
#include <grpcpp/security/credentials.h>

#include <fstream>
#include <streambuf>
#include <string>

namespace pingcap
{
struct ClusterConfig
{
    std::string tiflash_engine_key;
    std::string tiflash_engine_value;
    std::string ca_path;
    std::string cert_path;
    std::string key_path;

    ClusterConfig() = default;

    ClusterConfig(const std::string & engine_key_, const std::string & engine_value_, const std::string & ca_path_, const std::string & cert_path_, const std::string & key_path_)
        : tiflash_engine_key(engine_key_)
        , tiflash_engine_value(engine_value_)
        , ca_path(ca_path_)
        , cert_path(cert_path_)
        , key_path(key_path_)
    {}

    bool hasTlsConfig() const { return !ca_path.empty(); }


    grpc::SslCredentialsOptions getGrpcCredentials() const
    {
        if (hasTlsConfig())
        {
            grpc::SslCredentialsOptions options;
            options.pem_root_certs = readFile(ca_path);
            options.pem_cert_chain = readFile(cert_path);
            options.pem_private_key = readFile(key_path);
            return options;
        }
        return {};
    }

private:
    static std::string readFile(const std::string & path)
    {
        std::ifstream t(path.data());
        std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
        return str;
    }
};

} // namespace pingcap
