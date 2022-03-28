configure_for_external_kerberos() {
  echo "Setting krb5 config file"
  touch ${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty
  export KRB5_CONFIG=${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty

  if test -z "$KEYTAB_BASE64"; then
    echo KEYTAB_BASE64 must be set in the environment 1>&2
    exit 5
  fi

  echo "Writing keytab"
  echo "$KEYTAB_BASE64" | base64 --decode > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab

  if test -z "$PRINCIPAL"; then
    echo PRINCIPAL must be set in the environment 1>&2
    exit 5
  fi

  echo "Running kinit"
  kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p "$PRINCIPAL"

  # Realm must be uppercased.
  export SASL_REALM=`echo "$SASL_HOST" |tr a-z A-Z`
}

configure_local_kerberos() {
  # This configuration should only be run in a Docker environment
  # because it overwrites files in /etc.
  #
  # https://stackoverflow.com/questions/20010199/how-to-determine-if-a-process-runs-inside-lxc-docker
  if ! grep -q docker /proc/1/cgroup; then
    echo Local Kerberos configuration should only be done in Docker containers 1>&2
    exit 43
  fi

  cp .evergreen/local-kerberos/krb5.conf /etc/
  mkdir -p /etc/krb5kdc
  cp .evergreen/local-kerberos/kdc.conf /etc/krb5kdc/kdc.conf
  cp .evergreen/local-kerberos/kadm5.acl /etc/krb5kdc/

  cat .evergreen/local-kerberos/test.keytab.base64 |\
    base64 --decode > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab

  (echo masterp; echo masterp) |kdb5_util create -s
  (echo testp; echo testp) |kadmin.local addprinc rubytest@LOCALKRB

  krb5kdc
  kadmind

  echo 127.0.0.1 krb.local |tee -a /etc/hosts
  echo testp |kinit rubytest@LOCALKRB

  (echo hostp; echo hostp) |kadmin.local addprinc mongodb/`hostname`@LOCALKRB
  kadmin.local ktadd mongodb/`hostname`

  # Server is installed here in the Docker environment.
  export BINDIR=/opt/mongodb/bin
  if ! "$BINDIR"/mongod --version |grep enterprise; then
    echo MongoDB server is not an enterprise one 1>&2
    exit 44
  fi

  mkdir /db
  "$BINDIR"/mongod --dbpath /db --fork --logpath /db/mongod.log

  create_user_cmd="`cat <<'EOT'
    db.getSiblingDB("$external").runCommand(
      {
        createUser: "rubytest@LOCALKRB",
        roles: [
             { role: "root", db: "admin" },
        ],
        writeConcern: { w: "majority" , wtimeout: 5000 },
      }
    )
EOT
  `"

  "$BINDIR"/mongo --eval "$create_user_cmd"
  "$BINDIR"/mongo --eval 'db.getSiblingDB("kerberos").test.insert({kerberos: true, authenticated: "yeah"})'
  pkill mongod
  sleep 1

  # https://mongodb.com/docs/manual/tutorial/control-access-to-mongodb-with-kerberos-authentication/
  "$BINDIR"/mongod --dbpath /db --fork --logpath /db/mongod.log \
    --bind_ip 0.0.0.0 \
    --auth --setParameter authenticationMechanisms=GSSAPI &

  export SASL_USER=rubytest
  export SASL_PASS=testp
  export SASL_HOST=`hostname`
  export SASL_REALM=LOCALKRB
  export SASL_PORT=27017
  export SASL_DB='$external'
  export KERBEROS_DB=kerberos
}

configure_kerberos_ip_addr() {
  # TODO Find out of $OS is set here, right now we only test on Linux thus
  # it doesn't matter if it is set.
  case "$OS" in
    cygwin*)
      IP_ADDR=`getent hosts ${SASL_HOST} | head -n 1 | awk '{print $1}'`
      ;;

    darwin)
      IP_ADDR=`dig ${SASL_HOST} +short | tail -1`
      ;;

    *)
      IP_ADDR=`getent hosts ${SASL_HOST} | head -n 1 | awk '{print $1}'`
  esac

  export IP_ADDR
}
