#include <stdlib.h>
#include <string.h>

#include "bson_buffer.h"

#define INITIAL_BUFFER_SIZE 256
#define DEFAULT_MAX_SIZE 4 * 1024 * 1024

struct bson_buffer {
    char* buffer;
    int size;
    int position;
    int max_size;
};

/* Allocate and return a new buffer.
 * Return NULL on allocation failure. */
bson_buffer_t bson_buffer_new(void) {
    bson_buffer_t buffer;
    buffer = (bson_buffer_t)malloc(sizeof(struct bson_buffer));
    if (buffer == NULL) {
        return NULL;
    }

    buffer->size = INITIAL_BUFFER_SIZE;
    buffer->position = 0;
    buffer->buffer = (char*)malloc(sizeof(char) * INITIAL_BUFFER_SIZE);
    if (buffer->buffer == NULL) {
        free(buffer);
        return NULL;
    }
    buffer->max_size = DEFAULT_MAX_SIZE;

    return buffer;
}

void bson_buffer_set_max_size(bson_buffer_t buffer, int max_size) {
    buffer->max_size = max_size;
}

int bson_buffer_get_max_size(bson_buffer_t buffer) {
    return buffer->max_size;
}

/* Free the memory allocated for `buffer`.
 * Return non-zero on failure. */
int bson_buffer_free(bson_buffer_t buffer) {
    if (buffer == NULL) {
        return 1;
    }
    free(buffer->buffer);
    free(buffer);
    return 0;
}

/* Grow `buffer` to at least `min_length`.
 * Return non-zero on allocation failure. */
static int buffer_grow(bson_buffer_t buffer, int min_length) {
    int size = buffer->size;
    int old_size;
    char* old_buffer = buffer->buffer;
    if (size >= min_length) {
        return 0;
    }
    while (size < min_length) {
        old_size = size;
        size *= 2;
        /* Prevent potential overflow. */
        if( size < old_size )
            size = min_length;
    }
    buffer->buffer = (char*)realloc(buffer->buffer, sizeof(char) * size);
    if (buffer->buffer == NULL) {
        free(old_buffer);
        free(buffer);
        return 1;
    }
    buffer->size = size;
    return 0;
}

/* Assure that `buffer` has at least `size` free bytes (and grow if needed).
 * Return non-zero on allocation failure. */
static int buffer_assure_space(bson_buffer_t buffer, int size) {
    if (buffer->position + size <= buffer->size) {
        return 0;
    }
    return buffer_grow(buffer, buffer->position + size);
}

/* Save `size` bytes from the current position in `buffer` (and grow if needed).
 * Return offset for writing, or -1 on allocation failure. */
bson_buffer_position bson_buffer_save_space(bson_buffer_t buffer, int size) {
    int position = buffer->position;
    if (buffer_assure_space(buffer, size) != 0) {
        return -1;
    }
    buffer->position += size;
    return position;
}

/* Write `size` bytes from `data` to `buffer` (and grow if needed).
 * Return non-zero on allocation failure. */
int bson_buffer_write(bson_buffer_t buffer, const char* data, int size) {
    if (buffer_assure_space(buffer, size) != 0) {
        return 1;
    }

    memcpy(buffer->buffer + buffer->position, data, size);
    buffer->position += size;
    return 0;
}

/* Write `size` bytes from `data` to `buffer` at position `position`.
 * Does not change the internal position of `buffer`.
 * Return non-zero if buffer isn't large enough for write. */
int bson_buffer_write_at_position(bson_buffer_t buffer, bson_buffer_position position,
                             const char* data, int size) {
    if (position + size > buffer->size) {
        bson_buffer_free(buffer);
        return 1;
    }

    memcpy(buffer->buffer + position, data, size);
    return 0;
}


int bson_buffer_get_position(bson_buffer_t buffer) {
    return buffer->position;
}

char* bson_buffer_get_buffer(bson_buffer_t buffer) {
    return buffer->buffer;
}
