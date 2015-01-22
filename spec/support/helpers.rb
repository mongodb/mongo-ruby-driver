# Copyright (C) 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Helper methods and utilities for testing.
module Helpers

  # Helper method to allow temporary redirection of $stdout.
  #
  # @example
  # silence do
  #   # your noisey code here
  # end
  #
  # @param A code block to execute.
  # @return Original $stdout value.
  def silence(&block)
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = $stderr = File.new('/dev/null', 'w')
    yield block
  ensure
    $stdout = original_stdout
    $stderr = original_stderr
  end

  TEST_KEY_RSA1024 = OpenSSL::PKey::RSA.new <<-_end_of_pem_
-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDLwsSw1ECnPtT+PkOgHhcGA71nwC2/nL85VBGnRqDxOqjVh7Cx
aKPERYHsk4BPCkE3brtThPWc9kjHEQQ7uf9Y1rbCz0layNqHyywQEVLFmp1cpIt/
Q3geLv8ZD9pihowKJDyMDiN6ArYUmZczvW4976MU3+l54E6lF/JfFEU5hwIDAQAB
AoGBAKSl/MQarye1yOysqX6P8fDFQt68VvtXkNmlSiKOGuzyho0M+UVSFcs6k1L0
maDE25AMZUiGzuWHyaU55d7RXDgeskDMakD1v6ZejYtxJkSXbETOTLDwUWTn618T
gnb17tU1jktUtU67xK/08i/XodlgnQhs6VoHTuCh3Hu77O6RAkEA7+gxqBuZR572
74/akiW/SuXm0SXPEviyO1MuSRwtI87B02D0qgV8D1UHRm4AhMnJ8MCs1809kMQE
JiQUCrp9mQJBANlt2ngBO14us6NnhuAseFDTBzCHXwUUu1YKHpMMmxpnGqaldGgX
sOZB3lgJsT9VlGf3YGYdkLTNVbogQKlKpB8CQQDiSwkb4vyQfDe8/NpU5Not0fII
8jsDUCb+opWUTMmfbxWRR3FBNu8wnym/m19N4fFj8LqYzHX4KY0oVPu6qvJxAkEA
wa5snNekFcqONLIE4G5cosrIrb74sqL8GbGb+KuTAprzj5z1K8Bm0UW9lTjVDjDi
qRYgZfZSL+x1P/54+xTFSwJAY1FxA/N3QPCXCjPh5YqFxAMQs2VVYTfg+t0MEcJD
dPMQD5JX6g5HKnHFg2mZtoXQrWmJSn7p8GJK8yNTopEErA==
-----END RSA PRIVATE KEY-----
  _end_of_pem_

  def issue_cert(dn, key, serial, not_before, not_after, extensions, issuer, issuer_key, digest)
    cert = OpenSSL::X509::Certificate.new
    issuer = cert unless issuer
    issuer_key = key unless issuer_key
    cert.version = 2
    cert.serial = serial
    cert.subject = dn
    cert.issuer = issuer.subject
    cert.public_key = key.public_key
    cert.not_before = not_before
    cert.not_after = not_after
    ef = OpenSSL::X509::ExtensionFactory.new
    ef.subject_certificate = cert
    ef.issuer_certificate = issuer
    extensions.each do |oid, value, critical|
      cert.add_extension(ef.create_extension(oid, value, critical))
    end
    cert.sign(issuer_key, digest)
    cert
  end

  def collection(name, db)
    documents = []
    double(name.to_s).tap do |coll|

      allow(coll).to receive(:db) { db }

      allow(coll).to receive(:save) do |doc|
        coll.remove({ :_id => doc[:_id] })
        coll.insert(doc)
      end

      allow(coll).to receive(:count) { documents.length }

      allow(coll).to receive(:insert) do |doc|
        if !coll.find({ :_id => doc[:_id] }).empty?
          raise GridError, "duplicate key error, _id"
        end
        documents.push(doc)
      end

      allow(coll).to receive(:find_one) do |query|
        result = nil
        documents.each do |doc|
          if matches(doc, query)
            result = doc
            break
          end
        end
        result
      end

      allow(coll).to receive(:remove) do |query|
        documents.dup.each do |doc|
          if matches(doc, query)
            documents.delete(doc)
          end
        end
      end

      allow(coll).to receive(:find) do |query|
        results = []
        documents.each do |doc|
          if matches(doc, query)
            results.push(doc)
          end
        end
        results
      end
    end
  end

  # does only strict equivalence,
  # {:n => 4} should work
  # {:n => {"$gt" => 3}} will not work.
  def matches(doc, query={})
    match = true
    query.each do |field, value|
      if !doc[field] || doc[field] != value
        match = false
        break
      end
    end
    match
  end
end
