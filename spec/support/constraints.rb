# frozen_string_literal: true
# rubocop:todo all

module Constraints

  # Some tests hardcode the TLS certificates shipped with the driver's
  # test suite, and will fail when using TLS connections that use other
  # certificates.
  def require_local_tls
    require_tls

    before(:all) do
      # TODO This isn't actually the foolproof check
      if ENV['OCSP_ALGORITHM']
        skip 'Driver TLS certificate required, OCSP certificates are not acceptable'
      end
    end
  end

  def max_bson_version(version)
    required_version = version.split('.').map(&:to_i)
    actual_version = bson_version(required_version.length)
    before(:all) do
      if (actual_version <=> required_version) > 0
        skip "bson-ruby version #{version} or lower is required"
      end
    end
  end

  def bson_version(precision)
    BSON::VERSION.split('.')[0...precision].map(&:to_i)
  end
end
