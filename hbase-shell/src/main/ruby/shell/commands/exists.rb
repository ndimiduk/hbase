#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    class Exists < Command
      def help
        <<-EOF
Does the named table exist?
  hbase> exists 't1'
  hbase> exists 'ns1:t1'
EOF
      end

      def command(table)
        exists = admin.exists?(table.to_s)
        formatter.row([
                        "Table #{table} " + (exists ? 'does exist' : 'does not exist')
                      ])
        exists
      end
    end
  end
end
