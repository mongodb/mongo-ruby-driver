clear_instance_profile() {
  # The tests check, for example, failure to authenticate when no credentials
  # are explicitly provided. If an instance profile happens to be assigned
  # to the running instance, those tests will fail; clear instance profile
  # (if any) for regular and assume role configurations.
  #
  # To clear the instance profile, we need to use the EC2 credentials.
  # Set them in a subshell to ensure they are not accidentally leaked into
  # the main shell environment, which uses different credentials for
  # regular and assume role configurations.
  (
    # When running in Evergreen, credentials are written to this file.
    # In Docker they are already in the environment and the file does not exist.
    if test -f .env.private; then
      . ./.env.private
    fi
    
    export MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID="`get_var IAM_AUTH_EC2_INSTANCE_ACCOUNT`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY="`get_var IAM_AUTH_EC2_INSTANCE_SECRET_ACCESS_KEY`"
    export MONGO_RUBY_DRIVER_AWS_AUTH_INSTANCE_PROFILE_ARN="`get_var IAM_AUTH_EC2_INSTANCE_PROFILE`"
    # Region is not specified in Evergreen but can be specified when
    # testing locally.
    export MONGO_RUBY_DRIVER_AWS_AUTH_REGION=${MONGO_RUBY_DRIVER_AWS_AUTH_REGION:=us-east-1}
    
    ruby -Ispec -Ilib -I.evergreen/lib -rec2_setup -e Ec2Setup.new.clear_instance_profile
  )
}
