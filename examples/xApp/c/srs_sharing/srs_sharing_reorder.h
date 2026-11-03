/*
 * [srs-sharing] Puts each dApp's SRS sharing messages back in sequence order before the registry sees them.
 *
 * The near-RT RIC forwards E2 indications on more than one thread, so two messages a dApp sent a few milliseconds
 * apart can reach the xApp swapped (seen live at a UE reattach: 8 before 7, 10 before 9). Order matters to the
 * registry: a timing advance command handled before its random access value is wiped by it, a late configuration
 * report would replace a newer one, and a swap looks like a lost message.
 *
 * Per sender (node PLMN, gNB ID, dApp ID) messages leave in Header.sequence order. One that arrives early waits for
 * the missing ones, at most hold_us; then they are given up (the registry sees the jump and counts a gap) and a
 * straggler arriving later is dropped. A sequence starting again at 1 is a restarted sender.
 * Pure logic, no FlexRIC calls; test_srs_sharing_reorder.c covers it.
 */
#ifndef SRS_SHARING_REORDER_H
#define SRS_SHARING_REORDER_H

#include "srs_sharing_registry.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
  srs_source_t src;
  uint64_t sequence;
  uint8_t* data;  /* owned by whoever holds the item */
  size_t len;
} srs_item_t;

typedef struct {
  uint64_t delivered;
  uint64_t reordered;  /* items that had to wait for an earlier one */
  uint64_t given_up;   /* sequences never received within hold_us */
  uint64_t dropped;    /* stragglers after their sequence was given up, and duplicates */
} srs_reorder_stats_t;

typedef struct srs_reorder srs_reorder_t;

srs_reorder_t* srs_reorder_new(uint64_t hold_us);
void srs_reorder_free(srs_reorder_t* r);  /* frees the data of items still held */

/* Takes ownership of item.data (freed if the item is dropped). */
void srs_reorder_push(srs_reorder_t* r, srs_item_t item, uint64_t now_us);

/* The next item due, in sequence order per sender; false when none is due yet. The caller owns out->data. */
bool srs_reorder_pop(srs_reorder_t* r, uint64_t now_us, srs_item_t* out);

srs_reorder_stats_t srs_reorder_stats(srs_reorder_t const* r);

#endif
