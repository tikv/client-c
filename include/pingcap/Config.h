#pragma once
#include <grpcpp/security/credentials.h>

#include <fstream>
#include <streambuf>
#include <string>

namespace pingcap
{

struct ClusterConfig
{
    std::string learner_key;
    std::string learner_value;
    std::string ca_path;
    std::string cert_path;
    std::string key_path;

    ClusterConfig() {}

    ClusterConfig(const std::string & learner_key_, const std::string & learner_value_, const std::string & ca_path_,
        const std::string & cert_path_, const std::string & key_path_)
        : learner_key(learner_key_), learner_value(learner_value_), ca_path(ca_path_), cert_path(cert_path_), key_path(key_path_)
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
    std::string readFile(const std::string & path) const
    {
        std::ifstream t(path.data());
        std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
        return str;
    }
};

} // namespace pingcap
