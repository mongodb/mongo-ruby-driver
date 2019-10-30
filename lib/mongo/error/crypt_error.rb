# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Error

    # An error related to the libmongocrypt binding.
    #
    # @param [ Integer ] code The error code
    # @param [ String ] message The error message
    class CryptError < Mongo::Error
      attr_accessor :code

      def initialize(code, message)
        @code = code
        super("Code #{code}: #{message}")
      end
    end

    # A libmongocrypt error relating to the client
    class CryptClientError < CryptError; end

    # A libmongocrypt error relating to the KMS
    class CryptKmsError < CryptError; end
  end
end
