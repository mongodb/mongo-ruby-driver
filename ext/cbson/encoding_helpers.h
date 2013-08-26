/*
 * Copyright 2013 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef ENCODING_HELPERS_H
#define ENCODING_HELPERS_H

#include <unistd.h>

typedef enum {
    VALID_UTF8,
    INVALID_UTF8,
    HAS_NULL
} result_t;

/**
 * validate_utf8_encoding:
 * @utf8: A UTF-8 encoded string.
 * @utf8_len: The length of @utf8 in bytes.
 * @allow_null: 1 If '\0' is allowed within @utf8, excluding trailing \0.
 *
 * Validates that @utf8 is a valid UTF-8 string.
 *
 * If @allow_null is 1, then '\0' is allowed within @utf8_len bytes of @utf8.
 * Generally, this is bad practice since the main point of UTF-8 strings is
 * that they can be used with strlen() and friends. However, some languages
 * such as Python can send UTF-8 encoded strings with NUL's in them.
 *
 * Returns: enum indicating validity of @utf8.
 */
result_t
validate_utf8_encoding (const char  *utf8,
                        size_t      utf8_len,
                        int         allow_null);


#endif /* ENCODING_HELPERS_H */
