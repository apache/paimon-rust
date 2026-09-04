# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

foreach(required IN ITEMS MAIN_BUILD_DIR CONSUMER_SOURCE_DIR TEST_ROOT
                          CXX_COMPILER INSTALL_LIBDIR)
  if(NOT DEFINED ${required})
    message(FATAL_ERROR "missing -D${required}=...")
  endif()
endforeach()

set(test_prefix "${TEST_ROOT}/prefix")
set(consumer_build "${TEST_ROOT}/build")
file(REMOVE_RECURSE "${TEST_ROOT}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" --install "${MAIN_BUILD_DIR}"
          --prefix "${test_prefix}"
  RESULT_VARIABLE install_result
  OUTPUT_VARIABLE install_stdout
  ERROR_VARIABLE install_stderr)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR
          "install-tree setup failed:\n${install_stdout}\n${install_stderr}")
endif()

set(config_libdir_alias "${TEST_ROOT}/libdir-alias")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E create_symlink
          "${test_prefix}/${INSTALL_LIBDIR}" "${config_libdir_alias}"
  RESULT_VARIABLE alias_result
  ERROR_VARIABLE alias_stderr)
if(NOT alias_result EQUAL 0)
  message(FATAL_ERROR "package config alias setup failed:\n${alias_stderr}")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}"
          -S "${CONSUMER_SOURCE_DIR}"
          -B "${consumer_build}"
          "-DPaimonCpp_DIR=${config_libdir_alias}/cmake/PaimonCpp"
          "-DCMAKE_CXX_COMPILER=${CXX_COMPILER}"
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_stdout
  ERROR_VARIABLE configure_stderr)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR
          "install-tree consumer configure failed:\n${configure_stdout}\n${configure_stderr}")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}" --build "${consumer_build}"
  RESULT_VARIABLE build_result
  OUTPUT_VARIABLE build_stdout
  ERROR_VARIABLE build_stderr)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR
          "install-tree consumer build failed:\n${build_stdout}\n${build_stderr}")
endif()

execute_process(
  COMMAND "${consumer_build}/paimon_install_tree_consumer${CMAKE_EXECUTABLE_SUFFIX}"
  RESULT_VARIABLE run_result
  OUTPUT_VARIABLE run_stdout
  ERROR_VARIABLE run_stderr)
if(NOT run_result EQUAL 0)
  message(FATAL_ERROR
          "install-tree consumer run failed:\n${run_stdout}\n${run_stderr}")
endif()
