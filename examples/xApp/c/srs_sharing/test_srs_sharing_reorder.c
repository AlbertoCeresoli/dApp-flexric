/*
 * [srs-sharing] Tests of the per-sender reorder buffer (srs_sharing_reorder.c).
 */
#include "srs_sharing_reorder.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

static srs_source_t const A = {.plmn = {0x00, 0xf1, 0x10}, .nb_id = 411, .dapp_id = 101};
static srs_source_t const B = {.plmn = {0x00, 0xf1, 0x20}, .nb_id = 411, .dapp_id = 102};

static void push(srs_reorder_t* r, srs_source_t src, uint64_t seq, uint64_t now)
{
  srs_item_t it = {.src = src, .sequence = seq, .data = malloc(1), .len = 1};
  srs_reorder_push(r, it, now);
}

/* Pops everything due at `now`; writes the sequences to out and returns how many. */
static size_t pop_all(srs_reorder_t* r, uint64_t now, uint64_t* out)
{
  size_t n = 0;
  srs_item_t it;
  while (srs_reorder_pop(r, now, &it)) {
    out[n++] = it.sequence;
    free(it.data);
  }
  return n;
}

static void test_in_order_passes_through(void)
{
  srs_reorder_t* r = srs_reorder_new(200000);
  uint64_t out[8];
  for (uint64_t s = 5; s <= 7; ++s)
    push(r, A, s, 0);
  assert(pop_all(r, 0, out) == 3 && out[0] == 5 && out[1] == 6 && out[2] == 7);
  srs_reorder_free(r);
}

static void test_swapped_pair_is_put_back_in_order(void)
{
  srs_reorder_t* r = srs_reorder_new(200000);
  uint64_t out[8];
  push(r, A, 1, 0);
  assert(pop_all(r, 0, out) == 1);
  push(r, A, 3, 10);              /* 3 before 2, as seen live */
  assert(pop_all(r, 10, out) == 0); /* waits for 2 */
  push(r, A, 2, 20);
  assert(pop_all(r, 20, out) == 2 && out[0] == 2 && out[1] == 3);
  assert(srs_reorder_stats(r).reordered == 1 && srs_reorder_stats(r).given_up == 0);
  srs_reorder_free(r);
}

static void test_a_lost_message_is_given_up_after_the_hold(void)
{
  srs_reorder_t* r = srs_reorder_new(200000);
  uint64_t out[8];
  push(r, A, 1, 0);
  pop_all(r, 0, out);
  push(r, A, 3, 1000);
  assert(pop_all(r, 150000, out) == 0);
  assert(pop_all(r, 201000, out) == 1 && out[0] == 3);
  assert(srs_reorder_stats(r).given_up == 1);
  push(r, A, 2, 300000);          /* the straggler, too late: dropped */
  assert(pop_all(r, 300000, out) == 0 && srs_reorder_stats(r).dropped == 1);
  srs_reorder_free(r);
}

static void test_senders_are_independent_and_restarts_reset(void)
{
  srs_reorder_t* r = srs_reorder_new(200000);
  uint64_t out[8];
  push(r, A, 10, 0);
  push(r, B, 1, 0);
  push(r, A, 12, 0);              /* A waits for 11, B goes on */
  assert(pop_all(r, 0, out) == 2);
  push(r, A, 1, 10);              /* A restarted */
  assert(pop_all(r, 10, out) == 1 && out[0] == 1);
  push(r, A, 2, 20);
  assert(pop_all(r, 20, out) == 1 && out[0] == 2);
  srs_reorder_free(r);
}

static void test_duplicates_are_dropped(void)
{
  srs_reorder_t* r = srs_reorder_new(200000);
  uint64_t out[8];
  push(r, A, 1, 0);
  push(r, A, 3, 0);
  push(r, A, 3, 0);
  assert(pop_all(r, 0, out) == 1);
  push(r, A, 2, 0);
  assert(pop_all(r, 0, out) == 2 && out[0] == 2 && out[1] == 3);
  push(r, A, 2, 0);
  assert(pop_all(r, 0, out) == 0 && srs_reorder_stats(r).dropped == 2);
  srs_reorder_free(r);
}

static void test_a_full_buffer_drops_new_items_safely(void)
{
  srs_reorder_t* r = srs_reorder_new(200000);
  uint64_t out[80];
  push(r, A, 1, 0);
  pop_all(r, 0, out);
  for (uint64_t s = 3; s < 3 + 70; ++s)   /* 2 never comes: 64 held, 6 dropped */
    push(r, A, s, 0);
  assert(srs_reorder_stats(r).dropped == 6);
  assert(pop_all(r, 300000, out) == 64 && out[0] == 3 && out[63] == 66);
  srs_reorder_free(r);
}

int main(void)
{
  test_in_order_passes_through();
  test_swapped_pair_is_put_back_in_order();
  test_a_lost_message_is_given_up_after_the_hold();
  test_senders_are_independent_and_restarts_reset();
  test_duplicates_are_dropped();
  test_a_full_buffer_drops_new_items_safely();
  printf("SRS sharing reorder tests passed\n");
  return 0;
}
