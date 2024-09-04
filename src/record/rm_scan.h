#pragma once
#include <common/defs.h>
#include <memory>
class RmFileHandle;
class RmScan : public RecScan {
    std::shared_ptr<RmFileHandle> file_handle_;
    Rid rid_;

public:
    explicit RmScan(RmFileHandle* file_handle);
    explicit RmScan(std::shared_ptr<RmFileHandle> file_handle);
    void begin() override;
    void next() override;
    bool is_end() const override;
    Rid rid() const override;
};
