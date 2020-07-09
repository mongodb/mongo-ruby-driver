require_relative './spec_config'
require_relative './client_registry'

class SpecSetup
  def run
    if SpecConfig.instance.external_user?
      warn 'Skipping user creation because the set of users is fixed'
      return
    end

    # Create the root user administrator as the first user to be added to the
    # database. This user will need to be authenticated in order to add any
    # more users to any other databases.
    begin
      create_user(admin_unauthorized_client, SpecConfig.instance.root_user)
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
        if admin_authorized_test_client.with(database: 'admin').database.users.info(SpecConfig.instance.root_user.name).any?
          warn "Skipping root user creation, likely auth is enabled on cluster"
        else
          raise
        end
      else
        raise
      end
    end
    admin_unauthorized_client.close

    # Adds the test user to the test database with permissions on all
    # databases that will be used in the test suite.
    create_user(admin_authorized_test_client, SpecConfig.instance.test_user)
    admin_authorized_test_client.close
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

  def admin_unauthorized_client
    ClientRegistry.instance.global_client('admin_unauthorized').with(
      socket_timeout: 5, connect_timeout: 5,
    )
  end

  def admin_authorized_test_client
    ClientRegistry.instance.global_client('root_authorized').with(
      socket_timeout: 5, connect_timeout: 5,
    )
  end
end
