#pragma once

#include <stdexcept>
#include <string>

#define ALLIGN_TO_SIZE(SIZE, ALIGN_TO) (std::max((size_t)SIZE, std::min((size_t)SIZE%ALIGN_TO,(size_t)1)*(ALIGN_TO-(SIZE%ALIGN_TO)+SIZE)))

#define MESSAGE_WITH_SOURCE_INFO(MESSAGE_STRING) std::string("[") + std::string(__FILE__) + std::string(":") + std::string(xstr_macro(__LINE__)) + std::string("] ") + std::string(MESSAGE_STRING)

#define INVALID_FUNCTION_CALL throw std::runtime_error(std::string("[") + std::string(__FILE__) + std::string(":") + std::string(xstr_macro(__LINE__)) + std::string("] ") + std::string("Called ") + std::string(xstr_macro(__FUNCTION__)) + std::string(" on an object of type ") + std::string(typeid(*this).name()));

#define INVALID_BRANCH_TAKEN throw std::runtime_error(std::string("[") + std::string(__FILE__) + std::string(":") + std::string(xstr_macro(__LINE__)) + std::string("] ") + std::string("Invalid branch taken in ") + std::string(xstr_macro(__FUNCTION__)) + std::string(" on an object of type ") + std::string(typeid(*this).name()));

#define INVALID_BRANCH_TAKEN_NO_TYPE throw std::runtime_error(std::string("[") + std::string(__FILE__) + std::string(":") + std::string(xstr_macro(__LINE__)) + std::string("] ") + std::string("Invalid branch taken in ") + std::string(xstr_macro(__FUNCTION__)));

#define UNIMPLEMENTED_FUNCTION throw std::runtime_error(std::string("[") + std::string(__FILE__) + std::string(":") + std::string(xstr_macro(__LINE__)) + std::string("] ") + std::string("Called unimplemented function ") + std::string(xstr_macro(__FUNCTION__)) + std::string(" on an object of type ") + std::string(typeid(*this).name()));

#define DEBUG_VARIABLE_MESSAGE _Pragma("message (\"DEBUG MODIFICATION\")")
