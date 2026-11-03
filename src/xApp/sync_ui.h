/*
 * Licensed to the OpenAirInterface (OAI) Software Alliance under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The OpenAirInterface Software Alliance licenses this file to You under
 * the OAI Public License, Version 1.1  (the "License"); you may not use this file
 * except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.openairinterface.org/?page_id=698
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *-------------------------------------------------------------------------------
 * For more information about the OpenAirInterface (OAI) Software Alliance:
 *      contact@openairinterface.org
 */


#ifndef MIR_SYNCRONIZE_USER_INTERFACE_H
#define MIR_SYNCRONIZE_USER_INTERFACE_H 

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct{
  pthread_cond_t cv_sync; // = PTHREAD_COND_INITIALIZER;
  pthread_mutex_t mtx_sync; // = PTHREAD_MUTEX_INITIALIZER;
  int wait_ms;
  bool flag_sync; // = false;
  bool msg_ack; // = false;
  // [srs-sharing] Why the answer was not a success (NULL: success). Set by signal_failure_sync_ui, or by
  // cond_wait_sync_ui when no answer came in time. Static strings, never freed.
  char const* failure;
} sync_ui_t;

void init_sync_ui(sync_ui_t* s);

void free_sync_ui(sync_ui_t* s);

// [srs-sharing] Call before sending the request whose answer cond_wait_sync_ui then waits for: an answer that
// arrives before the wait starts is kept instead of being lost (which made the wait run to its timeout).
void arm_sync_ui(sync_ui_t* s);

void cond_wait_sync_ui(sync_ui_t* s, uint32_t ms);

void signal_sync_ui(sync_ui_t* s); 

// [srs-sharing] Like signal_sync_ui, for a negative answer (e.g. a control failure): the waiter sees `reason`.
void signal_failure_sync_ui(sync_ui_t* s, char const* reason);

// [srs-sharing] Strings in sync_ui_t.failure the xApps compare against.
#define SYNC_UI_NO_ANSWER "no answer"
#define SYNC_UI_NODE_NOT_CONNECTED "E2 node not connected"
#define SYNC_UI_REFUSED "refused by the E2 node"

#endif

