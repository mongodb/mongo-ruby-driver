# frozen_string_literal: true
# encoding: utf-8

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
end
