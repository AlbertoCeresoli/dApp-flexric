/*
 * [srs-sharing] Per-sender reorder buffer; see srs_sharing_reorder.h.
 */
#include "srs_sharing_reorder.h"

#include <stdlib.h>
#include <string.h>

#define MAX_SENDERS 64
#define MAX_HELD 64   /* per sender; beyond it new items are dropped */

typedef struct {
  srs_item_t item;
  uint64_t arrival_us;
} held_t;

typedef struct {
  srs_source_t src;
  bool started;
  uint64_t expected;  /* next sequence to deliver */
  held_t held[MAX_HELD];
  size_t nof_held;
} sender_t;

struct srs_reorder {
  uint64_t hold_us;
  sender_t senders[MAX_SENDERS];
  size_t nof_senders;
  srs_reorder_stats_t stats;
};

srs_reorder_t* srs_reorder_new(uint64_t hold_us)
{
  srs_reorder_t* r = calloc(1, sizeof(srs_reorder_t));
  if (r != NULL)
    r->hold_us = hold_us;
  return r;
}

void srs_reorder_free(srs_reorder_t* r)
{
  if (r == NULL)
    return;
  for (size_t s = 0; s < r->nof_senders; ++s)
    for (size_t i = 0; i < r->senders[s].nof_held; ++i)
      free(r->senders[s].held[i].item.data);
  free(r);
}

srs_reorder_stats_t srs_reorder_stats(srs_reorder_t const* r)
{
  return r->stats;
}

static bool same_sender(srs_source_t const* a, srs_source_t const* b)
{
  return memcmp(a->plmn, b->plmn, 3) == 0 && a->nb_id == b->nb_id && a->dapp_id == b->dapp_id;
}

static sender_t* sender_of(srs_reorder_t* r, srs_source_t const* src)
{
  for (size_t s = 0; s < r->nof_senders; ++s)
    if (same_sender(&r->senders[s].src, src))
      return &r->senders[s];
  if (r->nof_senders == MAX_SENDERS)
    return NULL;
  sender_t* s = &r->senders[r->nof_senders++];
  memset(s, 0, sizeof(*s));
  s->src = *src;
  return s;
}

static void drop_held(srs_reorder_t* r, sender_t* s)
{
  for (size_t i = 0; i < s->nof_held; ++i) {
    free(s->held[i].item.data);
    r->stats.dropped++;
  }
  s->nof_held = 0;
}

void srs_reorder_push(srs_reorder_t* r, srs_item_t item, uint64_t now_us)
{
  sender_t* s = sender_of(r, &item.src);
  if (s == NULL) {
    free(item.data);
    r->stats.dropped++;
    return;
  }
  if (!s->started || (item.sequence == 1 && s->expected > 1)) {
    /* First message of this sender, or the sender restarted: start from here. */
    drop_held(r, s);
    s->started = true;
    s->expected = item.sequence;
  }
  if (item.sequence < s->expected) {
    /* A straggler whose sequence was already given up, or a duplicate. */
    free(item.data);
    r->stats.dropped++;
    return;
  }
  for (size_t i = 0; i < s->nof_held; ++i)
    if (s->held[i].item.sequence == item.sequence) {
      free(item.data);
      r->stats.dropped++;
      return;
    }
  if (s->nof_held == MAX_HELD) {
    /* Full (a sender far ahead of a long gap, or pop not called): drop the new item; the held ones still leave
     * once their wait ends. */
    free(item.data);
    r->stats.dropped++;
    return;
  }
  if (item.sequence != s->expected)
    r->stats.reordered++;
  s->held[s->nof_held++] = (held_t){.item = item, .arrival_us = now_us};
}

bool srs_reorder_pop(srs_reorder_t* r, uint64_t now_us, srs_item_t* out)
{
  for (size_t si = 0; si < r->nof_senders; ++si) {
    sender_t* s = &r->senders[si];
    if (s->nof_held == 0)
      continue;
    size_t lowest = 0;
    for (size_t i = 1; i < s->nof_held; ++i)
      if (s->held[i].item.sequence < s->held[lowest].item.sequence)
        lowest = i;
    held_t const h = s->held[lowest];
    bool const in_order = h.item.sequence == s->expected;
    bool const waited_enough = now_us >= h.arrival_us + r->hold_us;
    if (!in_order && !waited_enough)
      continue;
    if (!in_order)
      r->stats.given_up += h.item.sequence - s->expected;
    s->held[lowest] = s->held[--s->nof_held];
    s->expected = h.item.sequence + 1;
    r->stats.delivered++;
    *out = h.item;
    return true;
  }
  return false;
}
