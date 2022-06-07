# This file contains functions pertaining to driver configuration in Evergreen.

show_local_instructions() {
  show_local_instructions_impl "$arch" \
    MONGODB_VERSION \
    TOPOLOGY \
    RVM_RUBY \
    AUTH \
    SSL \
    COMPRESSOR \
    FLE \
    FCV \
    MONGO_RUBY_DRIVER_LINT \
    RETRY_READS \
    RETRY_WRITES \
    WITH_ACTIVE_SUPPORT \
    SINGLE_MONGOS \
    BSON \
    MMAPV1 \
    STRESS \
    FORK \
    SOLO \
    OCSP_ALGORITHM \
    OCSP_STATUS \
    OCSP_DELEGATE \
    OCSP_MUST_STAPLE \
    OCSP_CONNECTIVITY \
    OCSP_VERIFIER \
    EXTRA_URI_OPTIONS \
    API_VERSION_REQUIRED
}
