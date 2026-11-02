/*
 * [srs-sharing] SRS sharing xApp: relays the UEs one gNB's dApp reports to the dApps of the other gNBs, so that they
 * can receive those UEs' SRS.
 *
 * Over E2SM-DAPP (SM ID 255) on every E2 node that offers it:
 *  - report style 1 (E3 data): payloads on E3 RAN function RAN_FUNCTION_SRS_SHARING (20) are SrsSharingMessages
 *    from reporting dApps (SrsConfigReport, TimingAdvanceIndicationMessage); other E3 RAN functions are ignored;
 *  - report style 2 (subscription map): which dApps are connected to each node;
 *  - control style 1: each resulting ForeignUeUpdate goes, as an E3 xApp control on RAN function 20, to every dApp
 *    connected to every node other than the reporting one. A dApp that does not take part ignores the control.
 *    A dApp that appears in a map gets the current state of every known UE at once.
 * The registry (srs_sharing_registry.c) keeps the UEs and their timing advance ledger and builds the updates.
 *
 * Threads: the indication callbacks run on FlexRIC's threads and only queue work; the main loop drains the queue,
 * sends the controls (control_sm_xapp_api blocks until the node answers) and every two seconds looks for new E2
 * nodes, so gNBs that connect after the xApp starts are included.
 *
 * Usage: xapp_srs_sharing -c <flexric.conf> -p <SM plugin directory>
 *        (or tools/run_xapp.sh xapp_srs_sharing); Ctrl-C stops it. SRS_XAPP_DURATION=<s> stops it after s seconds.
 */
#include "../../../../src/util/alg_ds/alg/defer.h"
#include "../../../../src/util/time_now_us.h"
#include "../../../../src/xApp/e42_xapp_api.h"

#include "srs_sharing.pb-c.h"
#include "srs_sharing_registry.h"

#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int const DAPP_SM_ID = 255;
static uint32_t const E3_RAN_FUNCTION = SRS_SHARING__V1__RAN_FUNCTION__RAN_FUNCTION_SRS_SHARING;
static uint32_t const STYLE_E3_DATA = 1;
static uint32_t const STYLE_SUBSCRIPTION_MAP = 2;

#define MAX_NODES 16
#define MAX_DAPPS 64
#define MAX_UPDATES 64

static volatile sig_atomic_t g_terminate = 0;

static void* signal_thread(void* arg)
{
  int sig;
  if (sigwait((sigset_t*)arg, &sig) == 0)
    g_terminate = 1;
  return NULL;
}

static uint64_t monotonic_us(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* ---- E2 node identity -------------------------------------------------------------------------------------
 * Indication headers name their node with TS 38.413 PLMN octets (BCD: MCC2|MCC1, MNC3|MCC3, MNC2|MNC1, MNC3 = F
 * for a 2-digit MNC) and the gNB ID; FlexRIC names nodes with decoded MCC/MNC. Both are turned into this key. */
typedef struct {
  uint16_t mcc;
  uint16_t mnc;
  uint8_t mnc_len; /* 2 or 3 digits: "01" and "001" are different MNCs */
  uint32_t nb_id;
} node_key_t;

static node_key_t key_from_header(uint8_t const plmn[3], uint32_t nb_id)
{
  uint16_t mcc = (plmn[0] & 0x0f) * 100 + (plmn[0] >> 4) * 10 + (plmn[1] & 0x0f);
  uint16_t mnc = (plmn[2] & 0x0f) * 10 + (plmn[2] >> 4);
  uint8_t mnc_len = 2;
  if ((plmn[1] >> 4) != 0x0f) {
    mnc = mnc * 10 + (plmn[1] >> 4);
    mnc_len = 3;
  }
  return (node_key_t){.mcc = mcc, .mnc = mnc, .mnc_len = mnc_len, .nb_id = nb_id};
}

static node_key_t key_from_node(global_e2_node_id_t const* id)
{
  return (node_key_t){.mcc = id->plmn.mcc, .mnc = id->plmn.mnc, .mnc_len = id->plmn.mnc_digit_len,
                      .nb_id = id->nb_id.nb_id};
}

/* Whether a UE (its serving cell, as SrsSharingMessages name it: PLMN digits and gNB ID) is served by this node. Both
 * parts are needed: different operators can use the same gNB ID (the two lab gNBs both use OCUDU's default, 411). */
static bool serves(node_key_t key, SrsSharing__V1__CellRef const* cell)
{
  char plmn[16]; /* MCC (3 digits) + MNC (2 or 3); sized for any 16-bit values so nothing is ever cut */
  snprintf(plmn, sizeof(plmn), "%03u%0*u", key.mcc, key.mnc_len == 3 ? 3 : 2, key.mnc);
  return cell != NULL && cell->plmn_identity != NULL && strcmp(cell->plmn_identity, plmn) == 0
         && cell->gnb_id == key.nb_id;
}

static bool same_node(node_key_t a, node_key_t b)
{
  return a.mcc == b.mcc && a.mnc == b.mnc && a.nb_id == b.nb_id;
}

/* ---- state shared with the callbacks (all under g_mtx) -------------------------------------------------- */

typedef struct work {
  srs_source_t src;
  uint8_t* data;
  size_t len;
  struct work* next;
} work_t;

typedef struct {
  node_key_t key;
  uint32_t dapps[MAX_DAPPS];
  size_t nof_dapps;
  bool known;              /* a map has arrived */
  uint32_t fresh[MAX_DAPPS];  /* dApps new in the latest map: they get a snapshot */
  size_t nof_fresh;
} dapp_map_t;

static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;
static work_t* g_head = NULL;
static work_t* g_tail = NULL;
static dapp_map_t g_maps[MAX_NODES];
static size_t g_nof_maps = 0;

static dapp_map_t* map_of(node_key_t key)
{
  for (size_t i = 0; i < g_nof_maps; ++i)
    if (same_node(g_maps[i].key, key))
      return &g_maps[i];
  if (g_nof_maps == MAX_NODES)
    return NULL;
  dapp_map_t* m = &g_maps[g_nof_maps++];
  memset(m, 0, sizeof(*m));
  m->key = key;
  return m;
}

/* Report style 1: queue SRS sharing payloads; ignore every other E3 RAN function. */
static void on_e3_data(sm_ag_if_rd_t const* rd)
{
  assert(rd != NULL && rd->type == INDICATION_MSG_AGENT_IF_ANS_V0);
  e2sm_dapp_ind_hdr_t const* hdr = &rd->ind.dapp.ind.hdr;
  e2sm_dapp_ind_msg_t const* msg = &rd->ind.dapp.ind.msg;
  if (hdr->format != FORMAT_1_E2SM_DAPP_IND_HDR || msg->format != FORMAT_1_E2SM_DAPP_IND_MSG)
    return;
  if (hdr->frmt_1.ran_function_id != E3_RAN_FUNCTION || msg->frmt_1.data == NULL || msg->frmt_1.data_size == 0)
    return;

  work_t* w = calloc(1, sizeof(work_t));
  assert(w != NULL);
  memcpy(w->src.plmn, hdr->frmt_1.node_plmn_id, 3);
  w->src.nb_id = hdr->frmt_1.node_nb_id;
  w->src.dapp_id = hdr->frmt_1.dapp_id;
  w->len = msg->frmt_1.data_size;
  w->data = malloc(w->len);
  assert(w->data != NULL);
  memcpy(w->data, msg->frmt_1.data, w->len);

  pthread_mutex_lock(&g_mtx);
  if (g_tail != NULL)
    g_tail->next = w;
  else
    g_head = w;
  g_tail = w;
  pthread_mutex_unlock(&g_mtx);
}

/* Report style 2: replace the node's dApp list, and remember which dApps are new. */
static void on_subscription_map(sm_ag_if_rd_t const* rd)
{
  assert(rd != NULL && rd->type == INDICATION_MSG_AGENT_IF_ANS_V0);
  e2sm_dapp_ind_hdr_t const* hdr = &rd->ind.dapp.ind.hdr;
  e2sm_dapp_ind_msg_t const* msg = &rd->ind.dapp.ind.msg;
  if (hdr->format != FORMAT_2_E2SM_DAPP_IND_HDR || msg->format != FORMAT_2_E2SM_DAPP_IND_MSG)
    return;
  node_key_t key = key_from_header(hdr->frmt_2.node_plmn_id, hdr->frmt_2.node_nb_id);
  dapp_e3_subscription_list_t const* list = &msg->frmt_2.dapp_e3_subs;

  pthread_mutex_lock(&g_mtx);
  dapp_map_t* m = map_of(key);
  if (m != NULL) {
    uint32_t old[MAX_DAPPS];
    size_t nof_old = m->nof_dapps;
    memcpy(old, m->dapps, sizeof(old));
    m->nof_dapps = 0;
    for (size_t i = 0; i < list->sz_dapp_e3_subscriptions && m->nof_dapps < MAX_DAPPS; ++i) {
      uint32_t id = list->dapp_e3_subscriptions[i].dapp_id;
      m->dapps[m->nof_dapps++] = id;
      bool seen = false;
      for (size_t j = 0; j < nof_old; ++j)
        seen = seen || old[j] == id;
      if (!seen && m->nof_fresh < MAX_DAPPS)
        m->fresh[m->nof_fresh++] = id;
    }
    m->known = true;
    printf("[SRS xApp] node %03u-%02u/%u: %zu dApp(s) connected\n", key.mcc, key.mnc, key.nb_id, m->nof_dapps);
  }
  pthread_mutex_unlock(&g_mtx);
}

/* ---- E2 nodes and subscriptions (main thread only) ------------------------------------------------------ */

typedef struct {
  global_e2_node_id_t id;
  node_key_t key;
  sm_ans_xapp_t e3_data;
  sm_ans_xapp_t map;
} node_t;

static node_t g_nodes[MAX_NODES];
static size_t g_nof_nodes = 0;

static bool offers_dapp(e2_node_connected_xapp_t const* n)
{
  for (size_t i = 0; i < n->len_rf; ++i)
    if (n->rf[i].id == DAPP_SM_ID)
      return true;
  return false;
}

static sm_ans_xapp_t subscribe(global_e2_node_id_t const* id, uint32_t style, sm_cb cb)
{
  dapp_sub_data_t sub = {0};
  sub.et.format = FORMAT_1_E2SM_DAPP_EV_TRIGGER_FORMAT;
  sub.action_def = calloc(1, sizeof(e2sm_dapp_action_def_t));
  assert(sub.action_def != NULL);
  sub.action_def->format = FORMAT_1_E2SM_DAPP_ACTION_DEF;
  sub.action_def->ric_style_type = style;
  sm_ans_xapp_t ans = report_sm_xapp_api((global_e2_node_id_t*)id, DAPP_SM_ID, &sub, cb);
  free_dapp_sub_data(&sub);
  return ans;
}

/* Subscribes to both report styles on every E2SM-DAPP node not seen before. */
static void add_new_nodes(void)
{
  e2_node_arr_xapp_t arr = e2_nodes_xapp_api();
  defer({ free_e2_node_arr_xapp(&arr); });
  for (size_t i = 0; i < arr.len; ++i) {
    e2_node_connected_xapp_t const* n = &arr.n[i];
    node_key_t key = key_from_node(&n->id);
    bool known = false;
    for (size_t j = 0; j < g_nof_nodes; ++j)
      known = known || same_node(g_nodes[j].key, key);
    if (known || !offers_dapp(n) || g_nof_nodes == MAX_NODES)
      continue;
    node_t* node = &g_nodes[g_nof_nodes];
    node->id = cp_global_e2_node_id(&n->id);
    node->key = key;
    node->e3_data = subscribe(&node->id, STYLE_E3_DATA, on_e3_data);
    node->map = subscribe(&node->id, STYLE_SUBSCRIPTION_MAP, on_subscription_map);
    if (!node->e3_data.success || !node->map.success) {
      printf("[SRS xApp] node %03u-%02u/%u: subscription failed\n", key.mcc, key.mnc, key.nb_id);
      free_global_e2_node_id(&node->id);
      continue;
    }
    g_nof_nodes++;
    printf("[SRS xApp] node %03u-%02u/%u: subscribed to E3 data and the dApp map\n", key.mcc, key.mnc, key.nb_id);
  }
}

static node_t* node_of(node_key_t key)
{
  for (size_t i = 0; i < g_nof_nodes; ++i)
    if (same_node(g_nodes[i].key, key))
      return &g_nodes[i];
  return NULL;
}

/* ---- sending ----------------------------------------------------------------------------------------- */

static uint64_t g_controls_ok = 0;
static uint64_t g_controls_failed = 0;

static void send_control(node_t* node, uint32_t dapp_id, srs_packed_t const* p)
{
  dapp_ctrl_req_data_t ctrl = {0};
  ctrl.hdr.format = FORMAT_1_E2SM_DAPP_CTRL_HDR;
  ctrl.hdr.frmt_1.ran_function_id = E3_RAN_FUNCTION;
  ctrl.hdr.frmt_1.dapp_id = dapp_id;
  ctrl.msg.format = FORMAT_1_E2SM_DAPP_CTRL_MSG;
  /* Raw payload, e3 NONE: the SM plugin encodes it as given. Not freed here: p owns the bytes. */
  ctrl.msg.frmt_1.data = p->buf;
  ctrl.msg.frmt_1.data_size = (uint32_t)p->len;
  ctrl.e3.type = DAPP_E3_SM_NONE;
  sm_ans_xapp_t ans = control_sm_xapp_api(&node->id, DAPP_SM_ID, &ctrl);
  if (ans.success)
    g_controls_ok++;
  else
    g_controls_failed++;
  printf("[SRS xApp]   -> node %03u-%02u/%u dApp %u: %s\n", node->key.mcc, node->key.mnc, node->key.nb_id, dapp_id,
         ans.success ? "ack" : "failure");
}

/* One recipient: a dApp on a node other than the reporter's. */
typedef struct {
  node_key_t node;
  uint32_t dapp_id;
} target_t;

static size_t targets_except(node_key_t source, target_t* out, size_t max_out)
{
  size_t n = 0;
  pthread_mutex_lock(&g_mtx);
  for (size_t i = 0; i < g_nof_maps; ++i)
    if (!same_node(g_maps[i].key, source))
      for (size_t j = 0; j < g_maps[i].nof_dapps && n < max_out; ++j)
        out[n++] = (target_t){.node = g_maps[i].key, .dapp_id = g_maps[i].dapps[j]};
  pthread_mutex_unlock(&g_mtx);
  return n;
}

static void describe(srs_packed_t const* p)
{
  SrsSharing__V1__SrsSharingMessage* m = srs_sharing__v1__srs_sharing_message__unpack(NULL, p->len, p->buf);
  if (m == NULL || m->foreign_ue_update == NULL)
    return;
  SrsSharing__V1__ForeignUeUpdate const* f = m->foreign_ue_update;
  SrsSharing__V1__CellRef const* c = f->ue->serving_cell;
  printf("[SRS xApp] ForeignUeUpdate #%" PRIu64 ": UE %s/%" PRIu64 "/PCI %u rnti 0x%04x epoch %" PRIu64,
         m->header->sequence, c->plmn_identity, c->gnb_id, c->phys_cell_id, f->ue->rnti, f->config_epoch);
  if (f->released)
    printf(" released");
  if (f->resource != NULL && f->resource->periodicity_and_offset != NULL)
    printf(" period %u offset %u", f->resource->periodicity_and_offset->period,
           f->resource->periodicity_and_offset->offset);
  if (f->anchor != NULL)
    printf(" anchor %u.%u", f->anchor->sfn, f->anchor->slot);
  if (f->timing_advance != NULL)
    printf(" N_TA %" PRId64 " steps (%u commands%s)", f->timing_advance->n_ta_steps, f->timing_advance->nof_commands,
           f->timing_advance->missed_command_suspected ? ", missed command suspected" : "");
  printf("\n");
  srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
}

static void deliver(srs_packed_t* updates, size_t n, node_key_t source)
{
  target_t targets[MAX_NODES * MAX_DAPPS];
  size_t nof_targets = targets_except(source, targets, MAX_NODES * MAX_DAPPS);
  for (size_t i = 0; i < n; ++i) {
    describe(&updates[i]);
    if (nof_targets == 0)
      printf("[SRS xApp]   no dApp on another node to send it to\n");
    for (size_t t = 0; t < nof_targets; ++t) {
      node_t* node = node_of(targets[t].node);
      if (node != NULL)
        send_control(node, targets[t].dapp_id, &updates[i]);
    }
    free(updates[i].buf);
  }
}

/* New dApps get every known UE, except those their own node serves. */
static void send_snapshots(srs_registry_t* reg)
{
  for (;;) {
    node_key_t node_key;
    uint32_t dapp_id;
    bool found = false;
    pthread_mutex_lock(&g_mtx);
    for (size_t i = 0; i < g_nof_maps && !found; ++i)
      if (g_maps[i].nof_fresh > 0) {
        node_key = g_maps[i].key;
        dapp_id = g_maps[i].fresh[--g_maps[i].nof_fresh];
        found = true;
      }
    pthread_mutex_unlock(&g_mtx);
    if (!found)
      return;
    node_t* node = node_of(node_key);
    if (node == NULL)
      continue;
    srs_packed_t updates[MAX_UPDATES];
    size_t n = srs_registry_snapshot(reg, monotonic_us(), updates, MAX_UPDATES);
    for (size_t i = 0; i < n; ++i) {
      SrsSharing__V1__SrsSharingMessage* m = srs_sharing__v1__srs_sharing_message__unpack(NULL, updates[i].len,
                                                                                         updates[i].buf);
      bool own = m != NULL && serves(node_key, m->foreign_ue_update->ue->serving_cell);
      if (m != NULL)
        srs_sharing__v1__srs_sharing_message__free_unpacked(m, NULL);
      if (!own) {
        printf("[SRS xApp] snapshot for new dApp %u on node %03u-%02u/%u\n", dapp_id, node_key.mcc, node_key.mnc,
               node_key.nb_id);
        describe(&updates[i]);
        send_control(node, dapp_id, &updates[i]);
      }
      free(updates[i].buf);
    }
  }
}

static void drain(srs_registry_t* reg)
{
  pthread_mutex_lock(&g_mtx);
  work_t* w = g_head;
  g_head = g_tail = NULL;
  pthread_mutex_unlock(&g_mtx);
  while (w != NULL) {
    srs_packed_t updates[MAX_UPDATES];
    size_t n = srs_registry_on_payload(reg, w->src, w->data, w->len, monotonic_us(), updates, MAX_UPDATES);
    deliver(updates, n, key_from_header(w->src.plmn, w->src.nb_id));
    work_t* next = w->next;
    free(w->data);
    free(w);
    w = next;
  }
}

static void discard_queue(void)
{
  pthread_mutex_lock(&g_mtx);
  work_t* w = g_head;
  g_head = g_tail = NULL;
  pthread_mutex_unlock(&g_mtx);
  while (w != NULL) {
    work_t* next = w->next;
    free(w->data);
    free(w);
    w = next;
  }
}

int main(int argc, char* argv[])
{
  fr_args_t args = init_fr_args(argc, argv);

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  int rc = pthread_sigmask(SIG_BLOCK, &set, NULL);
  assert(rc == 0);
  pthread_t sig_thr;
  rc = pthread_create(&sig_thr, NULL, signal_thread, &set);
  assert(rc == 0);

  char const* duration_env = getenv("SRS_XAPP_DURATION");
  double const duration_s = duration_env != NULL ? atof(duration_env) : 0.0;

  init_xapp_api(&args);
  sleep(1);

  srs_registry_t* reg = srs_registry_new();
  assert(reg != NULL);
  printf("[SRS xApp] relaying E3 RAN function %u between the dApps of different E2 nodes\n", E3_RAN_FUNCTION);

  uint64_t const start = monotonic_us();
  uint64_t next_scan = 0;
  while (!g_terminate && (duration_s <= 0 || monotonic_us() - start < (uint64_t)(duration_s * 1e6))) {
    if (monotonic_us() >= next_scan) {
      add_new_nodes();
      next_scan = monotonic_us() + 2000000;
    }
    drain(reg);
    send_snapshots(reg);
    usleep(10000);
  }

  srs_registry_stats_t s = srs_registry_stats(reg);
  printf("[SRS xApp] done: %" PRIu64 " messages, %" PRIu64 " refused, %" PRIu64 " sequence gaps, %" PRIu64
         " updates, controls %" PRIu64 " ack / %" PRIu64 " failed, %zu UEs\n",
         s.messages, s.invalid, s.sequence_gaps, s.updates, g_controls_ok, g_controls_failed,
         srs_registry_nof_ues(reg));

  for (size_t i = 0; i < g_nof_nodes; ++i) {
    rm_report_sm_xapp_api(g_nodes[i].e3_data.u.handle);
    rm_report_sm_xapp_api(g_nodes[i].map.u.handle);
  }
  sleep(1);
  while (try_stop_xapp_api() == false)
    usleep(1000);
  for (size_t i = 0; i < g_nof_nodes; ++i)
    free_global_e2_node_id(&g_nodes[i].id);
  discard_queue(); /* what arrived during shutdown: the xApp API is stopped, nothing can be sent */
  srs_registry_free(reg);
  return 0;
}
