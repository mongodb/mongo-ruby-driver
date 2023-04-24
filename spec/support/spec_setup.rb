# frozen_string_literal: true
# rubocop:todo all

require_relative './spec_config'
require_relative './client_registry'

class SpecSetup
  def run
    if SpecConfig.instance.external_user?
      warn 'Skipping user creation because the set of users is fixed'
      return
    end

    with_client do |client|
      # For historical reasons, the test suite always uses
      # password-authenticated users, even when authentication is not
      # requested in the configuration. When authentication is requested
      # and password authentication is used (i.e., not x509 and not kerberos),
      # a suitable user already exists (it's the one specified in the URI)
      # and no additional users are needed. In other cases, including x509
      # auth and kerberos, create the "root user".
      # TODO redo the test suite so that this password-authenticated root user
      # is not required and the test suite uses whichever user is specified
      # in the URI, which could be none.
      if !SpecConfig.instance.auth? || SpecConfig.instance.x509_auth?
        # Create the root user administrator as the first user to be added to the
        # database. This user will need to be authenticated in order to add any
        # more users to any other databases.
        begin
          create_user(client, SpecConfig.instance.root_user)
        rescue Mongo::Error::OperationFailure => e
          # When testing a cluster that requires auth, root user is already set up
          # and it is not creatable without auth.
          # Seems like every mongodb version has its own error message
          # for trying to make a user when not authenticated,
          # and prior to 4.0 or so the codes are supposedly not reliable either.
          # In order: 4.0, 3.6, 3.4 through 2.6
          if e.message =~ /command createUser requires authentication|there are no users authenticated|not authorized on admin to execute command.*createUser/
            # However, if the cluster is configured to require auth but
            # test suite has wrong credentials, then admin_authorized_test_client
            # won't be authenticated and the following line will raise an
            # exception
            if client.use('admin').database.users.info(SpecConfig.instance.root_user.name).any?
              warn "Skipping root user creation, likely auth is enabled on cluster"
            else
              raise
            end
          else
            raise
          end
        end
      end

      # Adds the test user to the test database with permissions on all
      # databases that will be used in the test suite.
      create_user(client, SpecConfig.instance.test_user)
    end
  end

  def create_user(client, user)
    users = client.use('admin').database.users
    begin
      users.create(user)
    rescue Mongo::Error::OperationFailure => e
      if e.message =~ /User.*already exists/
        users.remove(user.name)
        users.create(user)
      else
        raise
      end
    end
  end

  def with_client(&block)
    Mongo::Client.new(
      SpecConfig.instance.addresses,
      SpecConfig.instance.all_test_options.merge(
        socket_timeout: 5, connect_timeout: 5,
      ),
      &block
    )
  end
end
