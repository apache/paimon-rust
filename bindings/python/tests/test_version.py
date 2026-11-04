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
#

import re
from importlib.metadata import version

import pypaimon_rust


def test_module_exposes_installed_version():
    # Release candidates add an `rcN` suffix to the distribution version only,
    # so compare against the installed distribution with that suffix removed.
    expected = re.sub(r"rc\d+$", "", version("pypaimon-rust"))
    assert pypaimon_rust.__version__ == expected
