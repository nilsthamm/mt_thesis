#pragma once

class Reinsertable {
public:
    virtual bool reinsert() = 0;
    virtual int get_max_level() = 0;
};