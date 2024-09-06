
#ifndef _CS_PTR_HXX_
#define _CS_PTR_HXX_

#include <memory>

namespace nuraft {

template <typename T> using ptr = std::shared_ptr<T>;

template <typename T> using wptr = std::weak_ptr<T>;

template <typename T, typename... TArgs> inline ptr<T> new_ptr(TArgs&&... args) {
    return std::make_shared<T>(std::forward<TArgs>(args)...);
}

} // namespace nuraft
#endif //_CS_PTR_HXX_
