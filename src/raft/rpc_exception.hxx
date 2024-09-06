
#ifndef _RPC_EXCEPTION_HXX_
#define _RPC_EXCEPTION_HXX_

#include "pp_util.hxx"
#include "ptr.hxx"
#include <exception>
#include <string>

namespace nuraft {

class req_msg;
class rpc_exception : public std::exception {
public:
    rpc_exception(const std::string& err, ptr<req_msg> req)
        : req_(req)
        , err_(err.c_str()) {}

    __nocopy__(rpc_exception);

public:
    ptr<req_msg> req() const { return req_; }

    virtual const char* what() const throw() __override__ { return err_.c_str(); }

private:
    ptr<req_msg> req_;
    std::string err_;
};

} // namespace nuraft

#endif //_RPC_EXCEPTION_HXX_
