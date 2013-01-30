#ifndef _BSON_BUFFER_H
#define _BSON_BUFFER_H

/* Note: if any of these functions return a failure condition then the buffer
 * has already been freed. */

/* A buffer */
typedef struct bson_buffer* bson_buffer_t;
/* A position in the buffer */
typedef int bson_buffer_position;

/* Allocate and return a new buffer.
 * Return NULL on allocation failure. */
bson_buffer_t bson_buffer_new(void);

/* Set the max size for this buffer.
 * Note: this is not a hard limit. */
void bson_buffer_set_max_size(bson_buffer_t buffer, int max_size);
int bson_buffer_get_max_size(bson_buffer_t buffer);

/* Free the memory allocated for `buffer`.
 * Return non-zero on failure. */
int bson_buffer_free(bson_buffer_t buffer);

/* Save `size` bytes from the current position in `buffer` (and grow if needed).
 * Return offset for writing, or -1 on allocation failure. */
bson_buffer_position bson_buffer_save_space(bson_buffer_t buffer, int size);

/* Write `size` bytes from `data` to `buffer` (and grow if needed).
 * Return non-zero on allocation failure. */
int bson_buffer_write(bson_buffer_t buffer, const char* data, int size);

/* Write `size` bytes from `data` to `buffer` at position `position`.
 * Does not change the internal position of `buffer`.
 * Return non-zero if buffer isn't large enough for write. */
int bson_buffer_write_at_position(bson_buffer_t buffer, bson_buffer_position position, const char* data, int size);

/* Getters for the internals of a bson_buffer_t.
 * Should try to avoid using these as much as possible
 * since they break the abstraction. */
bson_buffer_position bson_buffer_get_position(bson_buffer_t buffer);
char* bson_buffer_get_buffer(bson_buffer_t buffer);

#endif
