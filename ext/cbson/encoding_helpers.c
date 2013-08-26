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


#include <string.h>
#include "encoding_helpers.h"


static void
get_utf8_sequence (const char       *utf8,
                   unsigned char    *seq_length,
                   unsigned char    *first_mask)
{
   unsigned char c = *(const unsigned char *)utf8;
   unsigned char m;
   unsigned char n;

   /*
    * See the following[1] for a description of what the given multi-byte
    * sequences will be based on the bits set of the first byte. We also need
    * to mask the first byte based on that.  All subsequent bytes are masked
    * against 0x3F.
    *
    * [1] http://www.joelonsoftware.com/articles/Unicode.html
    */

   if ((c & 0x80) == 0) {
      n = 1;
      m = 0x7F;
   } else if ((c & 0xE0) == 0xC0) {
      n = 2;
      m = 0x1F;
   } else if ((c & 0xF0) == 0xE0) {
      n = 3;
      m = 0x0F;
   } else if ((c & 0xF8) == 0xF0) {
      n = 4;
      m = 0x07;
   } else if ((c & 0xFC) == 0xF8) {
      n = 5;
      m = 0x03;
   } else if ((c & 0xFE) == 0xFC) {
      n = 6;
      m = 0x01;
   } else {
      n = 0;
      m = 0;
   }

   *seq_length = n;
   *first_mask = m;
}


result_t
validate_utf8_encoding (const char  *utf8,
                        size_t      utf8_len,
                        int         allow_null)
{
   unsigned char first_mask;
   unsigned char seq_length;
   unsigned i;
   unsigned j;

   for (i = 0; i < utf8_len; i += seq_length) {
      get_utf8_sequence(&utf8[i], &seq_length, &first_mask);
      if (!seq_length) {
         return INVALID_UTF8;
      }
      for (j = i + 1; j < (i + seq_length); j++) {
         if ((utf8[j] & 0xC0) != 0x80) {
            return INVALID_UTF8;
         }
      }
      if (!allow_null) {
         for (j = 0; j < seq_length; j++) {
            if (((i + j) > utf8_len) || !utf8[i + j]) {
               return HAS_NULL;
            }
         }
      }
   }

   return VALID_UTF8;
}
