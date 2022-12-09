# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
import pytest
from pathlib import PurePath
from api.v1.api_pb2 import SDK_JAVA

import checker
from checker import check_in_allowlist, check_sdk_examples


@pytest.mark.parametrize(
    "paths, allowlist, result",
    [
        ([PurePath("path1"), PurePath("path/path2")], [PurePath("path1")], True),
        ([PurePath("path1"), PurePath("path/path2")], [PurePath("path1")], True),
        ([PurePath("path1"), PurePath("path/path2")], [PurePath("path")], True),
        ([PurePath("path1"), PurePath("path/path2")], [PurePath("./path")], True),
        ([PurePath("path1"), PurePath("./path/path2")], [PurePath("path3")], False),
    ],
)
def test_check_in_allowlist(paths, allowlist, result):
    assert result == check_in_allowlist(paths, allowlist)


@pytest.mark.parametrize(
    "paths, sdk, has_tag, result",
    [
        ([PurePath("path"), PurePath("path/path2.java")], SDK_JAVA, True, True),
    ],
)
def test_check_sdk_examples(paths, sdk, has_tag, result):
    checker.get_tag = mock.Mock(return_value=has_tag)
    assert result == check_sdk_examples(paths, sdk, "root_dir")
