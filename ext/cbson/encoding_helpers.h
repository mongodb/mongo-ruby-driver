#ifndef ENCODING_HELPERS_H
#define ENCODING_HELPERS_H

typedef enum {
    VALID,
    NOT_UTF_8,
    HAS_NULL
} result_t;

result_t check_string(const unsigned char* string, const long length,
                      const char check_utf8, const char check_null);

#endif
