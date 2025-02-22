// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdlib>
#include <memory>
#include <string>


/** Demangles C++ symbol name.
  * When demangling fails, returns the original name and sets status to non-zero.
  * TODO: Write msvc version (now returns the same string)
  */
std::string demangle(const char * name, int & status);

inline std::string demangle(const char * name)
{
    int status = 0;
    return demangle(name, status);
}

// abi::__cxa_demangle returns a C string of known size that should be deleted
// with free().
struct FreeingDeleter
{
    template <typename PointerType>
    void operator()(PointerType ptr)
    {
        std::free(ptr); // NOLINT(cppcoreguidelines-no-malloc)
    }
};

using DemangleResult = std::unique_ptr<char, FreeingDeleter>;

DemangleResult tryDemangle(const char * name);
