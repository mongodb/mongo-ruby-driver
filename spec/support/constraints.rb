# frozen_string_literal: true

module Constraints
  # Some tests hardcode the TLS certificates shipped with the driver's
  # test suite, and will fail when using TLS connections that use other
  # certificates.
  def require_local_tls
    require_tls

    before(:all) do
      # TODO: This isn't actually the foolproof check
      skip 'Driver TLS certificate required, OCSP certificates are not acceptable' if ENV['OCSP_ALGORITHM']
    end
  end

  def minimum_mri_version(version)
    require_mri

    before(:all) do
      skip "Ruby #{version} or greater is required" if RUBY_VERSION < version
    end
  end

  def forbid_x509_auth
    before(:all) do
      skip 'X.509 auth not allowed' if SpecConfig.instance.x509_auth?
    end
  end

  def max_bson_version(version)
    required_version = version.split('.').map(&:to_i)
    actual_version = bson_version(required_version.length)
    before(:all) do
      skip "bson-ruby version #{version} or lower is required" if (actual_version <=> required_version) > 0
    end
  end

  def bson_version(precision)
    BSON::VERSION.split('.')[0...precision].map(&:to_i)
  end
end
