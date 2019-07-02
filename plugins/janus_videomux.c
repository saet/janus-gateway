/*! \file   janus_videomux.c
 * \author Fabrizio Bertone <bertone@ismb.it>
 * \copyright
 * \brief  Janus VideoMux plugin
 * \details This is a plugin implementing a DataChannel stream selection API.
 *
 * \section videomuxapi VideoMux API
 * TBD.
 *
 * \ingroup plugins
 * \ref plugins
 */

#include "plugin.h"
#include <jansson.h>
#include <curl/curl.h>
#include "../debug.h"
#include "../apierror.h"
#include "../mutex.h"
#include "../utils.h"

/* Plugin information */
#define JANUS_VIDEOMUX_VERSION			3
#define JANUS_VIDEOMUX_VERSION_STRING	"0.0.3"
#define JANUS_VIDEOMUX_DESCRIPTION		"This is a plugin implementing a stream selection API for Janus."
#define JANUS_VIDEOMUX_NAME				"JANUS VideoMux plugin"
#define JANUS_VIDEOMUX_AUTHOR			"ISMB <bertone@imsb.it>"
#define JANUS_VIDEOMUX_PACKAGE			"janus.plugin.videomux"

/* Plugin methods */
janus_plugin *create(void);
int janus_videomux_init(janus_callbacks *callback, const char *config_path);
void janus_videomux_destroy(void);
int janus_videomux_get_api_compatibility(void);
int janus_videomux_get_version(void);
const char *janus_videomux_get_version_string(void);
const char *janus_videomux_get_description(void);
const char *janus_videomux_get_name(void);
const char *janus_videomux_get_author(void);
const char *janus_videomux_get_package(void);
void janus_videomux_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_videomux_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_videomux_setup_media(janus_plugin_session *handle);
void janus_videomux_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_videomux_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_videomux_incoming_data(janus_plugin_session *handle, char *buf, int len);
void janus_videomux_slow_link(janus_plugin_session *handle, int uplink, int video);
void janus_videomux_hangup_media(janus_plugin_session *handle);
void janus_videomux_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_videomux_query_session(janus_plugin_session *handle);


/* Plugin setup */
static janus_plugin janus_videomux_plugin =
	JANUS_PLUGIN_INIT (
		.init = janus_videomux_init,
		.destroy = janus_videomux_destroy,

		.get_api_compatibility = janus_videomux_get_api_compatibility,
		.get_version = janus_videomux_get_version,
		.get_version_string = janus_videomux_get_version_string,
		.get_description = janus_videomux_get_description,
		.get_name = janus_videomux_get_name,
		.get_author = janus_videomux_get_author,
		.get_package = janus_videomux_get_package,

		.create_session = janus_videomux_create_session,
		.handle_message = janus_videomux_handle_message,
		.setup_media = janus_videomux_setup_media,
		.incoming_rtp = janus_videomux_incoming_rtp,
		.incoming_rtcp = janus_videomux_incoming_rtcp,
		.incoming_data = janus_videomux_incoming_data,
		.slow_link = janus_videomux_slow_link,
		.hangup_media = janus_videomux_hangup_media,
		.destroy_session = janus_videomux_destroy_session,
		.query_session = janus_videomux_query_session,
	);

/* Plugin creator */
janus_plugin *create(void) {
	JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_VIDEOMUX_NAME);
	return &janus_videomux_plugin;
}

/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

static struct janus_json_parameter transaction_parameters[] = {
	{"videomux", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
	{"transaction", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

static struct janus_json_parameter watch_parameters[] = {
	{"cam", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE},
	{"hash", JSON_STRING},
	{"sens", JSON_STRING},
	{"time", JSON_STRING},
	{"ack", JANUS_JSON_BOOL, 0}
};

/* Useful stuff */
static volatile gint initialized = 0, stopping = 0;
// static gboolean notify_events = TRUE;
static janus_callbacks *gateway = NULL;
static GThread *handler_thread;
static GThread *watchdog;
static void *janus_videomux_handler(void *data);
static void janus_videomux_hangup_media_internal(janus_plugin_session *handle);

/* JSON serialization options */
static size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;

typedef struct janus_videomux_message {
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;
} janus_videomux_message;

/* https://stackoverflow.com/questions/2329571/c-libcurl-get-output-into-a-string */
typedef struct string {
  char *ptr;
  size_t len;
} string;

int init_string(string *s);
size_t writefunc(void *ptr, size_t size, size_t nmemb, string *s);

int init_string(string *s) {
  s->len = 0;
  s->ptr = malloc(s->len+1);
  if (s->ptr == NULL) {
    return -1;
  }
  s->ptr[0] = '\0';
	return 0;
}

size_t writefunc(void *ptr, size_t size, size_t nmemb, string *s)
{
  size_t new_len = s->len + size*nmemb;
  s->ptr = realloc(s->ptr, new_len+1);
  if (s->ptr == NULL) {
    return -1;
  }
  memcpy(s->ptr+s->len, ptr, size*nmemb);
  s->ptr[new_len] = '\0';
  s->len = new_len;

  return size*nmemb;
}


static GAsyncQueue *messages = NULL;
static janus_videomux_message exit_message;

static void janus_videomux_message_free(janus_videomux_message *msg) {
	if(!msg || msg == &exit_message)
		return;

	msg->handle = NULL;

	g_free(msg->transaction);
	msg->transaction = NULL;
	if(msg->message)
		json_decref(msg->message);
	msg->message = NULL;
	if(msg->jsep)
		json_decref(msg->jsep);
	msg->jsep = NULL;

	g_free(msg);
}

typedef struct janus_videomux_session {
	janus_plugin_session *handle;
	gint64 sdp_sessid;
	gint64 sdp_version;
	janus_mutex mutex;			/* Mutex to lock this session */
	volatile gint setup;
	volatile gint hangingup;
	gint64 destroyed;	/* Time at which this session was marked as destroyed */
	int slot;
} janus_videomux_session;

/* video slots for muxer */
/*
static janus_mutex mux_mutex = JANUS_MUTEX_INITIALIZER;
static gboolean full_booked = FALSE;
static gboolean slots[4] = {};
*/

static GHashTable *sessions;
static GList *old_sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;

/* SDP template: we only offer data channels */
#define sdp_template \
		"v=0\r\n" \
		"o=- %"SCNu64" %"SCNu64" IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
		"s=Janus VideoMux plugin\r\n" \
		"t=0 0\r\n" \
		"m=application 1 DTLS/SCTP 5000\r\n" \
		"c=IN IP4 1.1.1.1\r\n" \
		"a=sctpmap:5000 webrtc-datachannel 16\r\n"

/* Error codes */
#define JANUS_VIDEOMUX_ERROR_NO_MESSAGE			411
#define JANUS_VIDEOMUX_ERROR_INVALID_JSON		412
#define JANUS_VIDEOMUX_ERROR_MISSING_ELEMENT	413
#define JANUS_VIDEOMUX_ERROR_INVALID_ELEMENT	414
#define JANUS_VIDEOMUX_ERROR_INVALID_REQUEST	415
#define JANUS_VIDEOMUX_ERROR_ALREADY_SETUP		416
#define JANUS_VIDEOMUX_ERROR_UNAUTHORIZED		419
#define JANUS_VIDEOMUX_ERROR_NO_SUCH_CAM			423
#define JANUS_VIDEOMUX_ERROR_NO_SLOT			424
#define JANUS_VIDEOMUX_ERROR_NO_FREE_SLOT			425
#define JANUS_VIDEOMUX_ERROR_CONNECTION_ERROR			426
#define JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR		499

/* VideoMux service url */
#define SERVICE_URL "http://127.0.0.1:1984"
//#define SERVICE_URL "http://192.168.1.75:1984"

/* CURL handler (da mettere come static?)*/
//static CURL *curl;

/* VideoMux watchdog/garbage collector (sort of) */
static void *janus_videomux_watchdog(void *data) {
	JANUS_LOG(LOG_INFO, "VideoMux watchdog started\n");
	gint64 now = 0;
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		janus_mutex_lock(&sessions_mutex);
		/* Iterate on all the sessions */
		now = janus_get_monotonic_time();
		if(old_sessions != NULL) {
			GList *sl = old_sessions;
			JANUS_LOG(LOG_HUGE, "Checking %d old VideoMux sessions...\n", g_list_length(old_sessions));
			while(sl) {
				janus_videomux_session *session = (janus_videomux_session *)sl->data;
				if(!session) {
					sl = sl->next;
					continue;
				}
				if(now-session->destroyed >= 5*G_USEC_PER_SEC) {
					/* We're lazy and actually get rid of the stuff only after a few seconds */
					JANUS_LOG(LOG_VERB, "Freeing old VideoMux session\n");
					GList *rm = sl->next;
					old_sessions = g_list_delete_link(old_sessions, sl);
					sl = rm;
					session->handle = NULL;
					/* TODO Free session stuff */
					g_free(session);
					session = NULL;
					continue;
				}
				sl = sl->next;
			}
		}
		janus_mutex_unlock(&sessions_mutex);
		g_usleep(500000);
	}
	JANUS_LOG(LOG_INFO, "VideoMux watchdog stopped\n");
	return NULL;
}

/* We use this method to handle incoming requests. Since most of the requests
 * will arrive from data channels, but some may also arrive from the regular
 * plugin messaging, we have the ability to pass
 * parsed JSON objects instead of strings, which explains why we specify a
 * janus_plugin_result pointer as a return value; messages handles via
 * datachannels would simply return NULL. Besides, some requests are actually
 * originated internally, and don't need any response to be sent to anyone,
 * which is what the additional boolean "internal" value is for */
janus_plugin_result *janus_videomux_handle_incoming_request(janus_plugin_session *handle,
	char *text, json_t *json, gboolean internal);

/* Plugin implementation */
int janus_videomux_init(janus_callbacks *callback, const char *config_path) {
	if(g_atomic_int_get(&stopping)) {
		/* Still stopping from before */
		return -1;
	}
	if(callback == NULL) {
		/* Invalid arguments */
		return -1;
	}

	/* init CURL for VideoMux API requests */
	curl_global_init(CURL_GLOBAL_ALL);

	// CURL *curl;
	// curl = curl_easy_init();
	// if(curl == NULL) {
	// 	JANUS_LOG(LOG_ERR, "Can't init CURL\n");
	// 	return -1;
	// }

	sessions = g_hash_table_new(NULL, NULL);
	messages = g_async_queue_new_full((GDestroyNotify) janus_videomux_message_free);
	/* This is the callback we'll need to invoke to contact the gateway */
	gateway = callback;

	g_atomic_int_set(&initialized, 1);

	GError *error = NULL;
	/* Start the sessions watchdog */
	watchdog = g_thread_try_new("VideoMux watchdog", &janus_videomux_watchdog, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the VideoMux watchdog thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}
	/* Launch the thread that will handle incoming messages */
	handler_thread = g_thread_try_new("VideoMux handler", janus_videomux_handler, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the VideoMux handler thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}
	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_VIDEOMUX_NAME);
	return 0;
}

void janus_videomux_destroy(void) {
	if(!g_atomic_int_get(&initialized))
		return;
	g_atomic_int_set(&stopping, 1);

	g_async_queue_push(messages, &exit_message);
	if(handler_thread != NULL) {
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	if(watchdog != NULL) {
		g_thread_join(watchdog);
		watchdog = NULL;
	}

	/* FIXME We should destroy the sessions cleanly */
	janus_mutex_lock(&sessions_mutex);
	g_hash_table_destroy(sessions);
	janus_mutex_unlock(&sessions_mutex);
	g_async_queue_unref(messages);
	messages = NULL;
	sessions = NULL;

	// CURL *curl;
	// curl = curl_easy_init();
	// if(curl == NULL) {
	// 	JANUS_LOG(LOG_ERR, "Can't init CURL\n");
	// 	return -1;
	// }
	// curl_easy_cleanup(curl);
	curl_global_cleanup();

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);
	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_VIDEOMUX_NAME);
}

int janus_videomux_get_api_compatibility(void) {
	/* Important! This is what your plugin MUST always return: don't lie here or bad things will happen */
	return JANUS_PLUGIN_API_VERSION;
}

int janus_videomux_get_version(void) {
	return JANUS_VIDEOMUX_VERSION;
}

const char *janus_videomux_get_version_string(void) {
	return JANUS_VIDEOMUX_VERSION_STRING;
}

const char *janus_videomux_get_description(void) {
	return JANUS_VIDEOMUX_DESCRIPTION;
}

const char *janus_videomux_get_name(void) {
	return JANUS_VIDEOMUX_NAME;
}

const char *janus_videomux_get_author(void) {
	return JANUS_VIDEOMUX_AUTHOR;
}

const char *janus_videomux_get_package(void) {
	return JANUS_VIDEOMUX_PACKAGE;
}

static janus_videomux_session *janus_videomux_lookup_session(janus_plugin_session *handle) {
	janus_videomux_session *session = NULL;
	if (g_hash_table_contains(sessions, handle)) {
		session = (janus_videomux_session *)handle->plugin_handle;
	}
	return session;
}

void janus_videomux_create_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_videomux_session *session = (janus_videomux_session *)g_malloc0(sizeof(janus_videomux_session));
	session->handle = handle;
	session->destroyed = 0;
	session->slot = 0;
	janus_mutex_init(&session->mutex);
	g_atomic_int_set(&session->setup, 0);
	g_atomic_int_set(&session->hangingup, 0);
	handle->plugin_handle = session;
	janus_mutex_lock(&sessions_mutex);
	g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

void janus_videomux_destroy_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_videomux_session *session = janus_videomux_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		*error = -2;
		return;
	}
	if(!session->destroyed) {
		JANUS_LOG(LOG_VERB, "Removing VideoMux session...\n");
		janus_videomux_hangup_media_internal(handle);
		session->destroyed = janus_get_monotonic_time();
		g_hash_table_remove(sessions, handle);
		/* Cleaning up and removing the session is done in a lazy way */
		old_sessions = g_list_append(old_sessions, session);
	}
	janus_mutex_unlock(&sessions_mutex);
	return;
}

json_t *janus_videomux_query_session(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		return NULL;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_videomux_session *session = janus_videomux_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return NULL;
	}

	json_t *info = json_object();
	json_object_set_new(info, "destroyed", json_integer(session->destroyed));
	json_object_set_new(info, "slot", json_integer(session->slot));
	janus_mutex_unlock(&sessions_mutex);
	return info;
}

/* messages from REST API */
struct janus_plugin_result *janus_videomux_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, g_atomic_int_get(&stopping) ? "Shutting down" : "Plugin not initialized", NULL);

	/* Pre-parse the message */
	int error_code = 0;
	char error_cause[512];
	json_t *root = message;
	json_t *response = NULL;

	janus_mutex_lock(&sessions_mutex);

	if(message == NULL) {
		JANUS_LOG(LOG_ERR, "No message??\n");
		error_code = JANUS_VIDEOMUX_ERROR_NO_MESSAGE;
		g_snprintf(error_cause, 512, "%s", "No message??");
		goto plugin_response;
	}

	janus_videomux_session *session = janus_videomux_lookup_session(handle);
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		error_code = JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "session associated with this handle...");
		goto plugin_response;
	}
	if(session->destroyed) {
		JANUS_LOG(LOG_ERR, "Session has already been destroyed...\n");
		error_code = JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "Session has already been destroyed...");
		goto plugin_response;
	}
	if(!json_is_object(root)) {
		JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
		error_code = JANUS_VIDEOMUX_ERROR_INVALID_JSON;
		g_snprintf(error_cause, 512, "JSON error: not an object");
		goto plugin_response;
	}
	/* Get the request first */
	JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
		error_code, error_cause, TRUE,
		JANUS_VIDEOMUX_ERROR_MISSING_ELEMENT, JANUS_VIDEOMUX_ERROR_INVALID_ELEMENT);
	if(error_code != 0)
		goto plugin_response;
	json_t *request = json_object_get(root, "request");
	/* Some requests (e.g., 'create' and 'destroy') can be handled synchronously */
	const char *request_text = json_string_value(request);

	if(!strcasecmp(request_text, "setup") || !strcasecmp(request_text, "ack") || !strcasecmp(request_text, "restart")) {
		/* These messages are handled asynchronously */
		janus_mutex_unlock(&sessions_mutex);
		janus_videomux_message *msg = g_malloc0(sizeof(janus_videomux_message));
		msg->handle = handle;
		msg->transaction = transaction;
		msg->message = root;
		msg->jsep = jsep;

		g_async_queue_push(messages, msg);

		return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
	} else if(!strcasecmp(request_text, "videomux")) {
		/* These messages are handled synchronously */
		janus_mutex_unlock(&sessions_mutex);
		return janus_videomux_handle_incoming_request(handle, NULL, root, FALSE);
	} else {
		JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
		error_code = JANUS_VIDEOMUX_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
	}

plugin_response:
		{
			janus_mutex_unlock(&sessions_mutex);
			if(!response) {
				/* Prepare JSON error event */
				response = json_object();
				json_object_set_new(response, "videomux", json_string("event"));
				json_object_set_new(response, "error_code", json_integer(error_code));
				json_object_set_new(response, "error", json_string(error_cause));
			}
			if(root != NULL)
				json_decref(root);
			if(jsep != NULL)
				json_decref(jsep);
			g_free(transaction);

			return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, response);
		}

}

void janus_videomux_setup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "WebRTC media is now available\n");
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_mutex_lock(&sessions_mutex);
	janus_videomux_session *session = janus_videomux_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	if(session->destroyed) {
		janus_mutex_unlock(&sessions_mutex);
		return;
	}
	g_atomic_int_set(&session->hangingup, 0);
	janus_mutex_unlock(&sessions_mutex);
}

void janus_videomux_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len) {
	/* We don't do audio/video */
}

void janus_videomux_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len) {
	/* We don't do audio/video */
}

/* datachannel */
void janus_videomux_incoming_data(janus_plugin_session *handle, char *buf, int len) {
	if(handle == NULL || handle->stopped || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	/* Incoming request from this user: what should we do? */
	janus_videomux_session *session = (janus_videomux_session *)handle->plugin_handle;
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	if(session->destroyed)
		return;
	if(buf == NULL || len <= 0)
		return;
	char *text = g_malloc0(len+1);
	memcpy(text, buf, len);
	*(text+len) = '\0';
	JANUS_LOG(LOG_INFO, "VideoMUX: Got a DataChannel message (%zu bytes): %s\n", strlen(text), text);
	janus_videomux_handle_incoming_request(handle, text, NULL, FALSE);
}

janus_plugin_result *janus_videomux_handle_incoming_request(janus_plugin_session *handle, char *text, json_t *json, gboolean internal) {
	janus_videomux_session *session = (janus_videomux_session *)handle->plugin_handle;

	int error_code = 0;
	char error_cause[512];

	CURL *curl;
	curl = curl_easy_init();
	if(curl == NULL) {
		JANUS_LOG(LOG_ERR, "Can't init CURL\n");
		error_code = JANUS_VIDEOMUX_ERROR_CONNECTION_ERROR;
		g_snprintf(error_cause, 512, "Connection error (curl_easy_init)");
		goto msg_response;
	}

	CURLcode res;
	char vvurl[128];

	/* Parse JSON, if needed */
	json_error_t error;
	json_t *root = text ? json_loads(text, 0, &error) : json;
	g_free(text);
	if(!root) {
		JANUS_LOG(LOG_ERR, "Error parsing data channel message (JSON error: on line %d: %s)\n", error.line, error.text);

		error_code = JANUS_VIDEOMUX_ERROR_INVALID_JSON;
		g_snprintf(error_cause, 512, "Invalid JSON");
		goto msg_response;
	}

	/* Handle request */

	JANUS_VALIDATE_JSON_OBJECT(root, transaction_parameters,
		error_code, error_cause, TRUE,
		JANUS_VIDEOMUX_ERROR_MISSING_ELEMENT, JANUS_VIDEOMUX_ERROR_INVALID_ELEMENT);
	const char *transaction_text = NULL;
	json_t *reply = NULL;
	if(error_code != 0)
		goto msg_response;

	json_t *request = json_object_get(root, "videomux");
	json_t *transaction = json_object_get(root, "transaction");
	const char *request_text = json_string_value(request);
	transaction_text = json_string_value(transaction);

	/* Start preparing the response too */
	reply = json_object();

	if(!strcasecmp(request_text, "open")) {

		if (session->slot > 0) {
			JANUS_LOG(LOG_WARN, "VideoMUX: OPEN session %llu, already opened with slot %d\n", session->sdp_sessid, session->slot);
			json_object_set_new(reply, "videomux", json_string("success"));
			json_object_set_new(reply, "slot", json_integer(session->slot));
		} else {
			JANUS_LOG(LOG_INFO, "VideoMUX: OPEN session %llu\n", session->sdp_sessid);

			struct string s;
			int init_res;
  		init_res = init_string(&s);
			if (init_res >= 0) {
				sprintf(vvurl, "%s/session/%llu", SERVICE_URL, session->sdp_sessid);
				curl_easy_setopt(curl, CURLOPT_URL, vvurl);
				curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
  			curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
				res = curl_easy_perform(curl);
				curl_easy_cleanup(curl);
				/* Check for errors */
		    if(res != CURLE_OK) {
		      fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
					error_code = JANUS_VIDEOMUX_ERROR_CONNECTION_ERROR;
					g_snprintf(error_cause, 512, "Connection error (curl_easy_perform)");
					free(s.ptr);
					goto msg_response;
				} else {
					JANUS_LOG(LOG_INFO, "\t\tVideoMUX: GOT OPEN response: %s\n", s.ptr);

					json_error_t error;
					json_t *jresp = json_loads(s.ptr, 0, &error);
					free(s.ptr);

					if(!jresp) {
						JANUS_LOG(LOG_ERR, "Error parsing data VideoMux message (JSON error: on line %d: %s)\n", error.line, error.text);

						error_code = JANUS_VIDEOMUX_ERROR_CONNECTION_ERROR;
						g_snprintf(error_cause, 512, "Connection error (invalid JSON)");
						goto msg_response;
					}

					json_t *jerror = json_object_get(jresp, "error");
					json_t *jslot = json_object_get(jresp, "slot");

					if (jerror == NULL && jslot != NULL) {
						int my_slot = json_integer_value(jslot);
						JANUS_LOG(LOG_INFO, "\t\t*** VideoMUX: GOT SLOT: %d\n", my_slot);
						session->slot = my_slot;
						json_object_set_new(reply, "videomux", json_string("success"));
						json_object_set_new(reply, "slot", json_integer(my_slot));

					} else {
						if (jerror != NULL) {
							const char *error_resp = json_string_value(jerror);
							JANUS_LOG(LOG_WARN, "\t\tVideoMUX: ERROR response: %s\n", error_resp);

							error_code = JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR;
							g_snprintf(error_cause, 512, "%s", error_resp);
							goto msg_response;
						} else {
							JANUS_LOG(LOG_WARN, "\t\tVideoMUX: OPEN response: no slot!\n");

							error_code = JANUS_VIDEOMUX_ERROR_NO_SLOT;
							g_snprintf(error_cause, 512, "No slot");
							goto msg_response;
						}
					}

  				// free(s.ptr);

				}

			} else {
				JANUS_LOG(LOG_ERR, "Can't init string\n");
				error_code = JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR;
				g_snprintf(error_cause, 512, "Memory error");
				goto msg_response;
			}
		}
	} else if(!strcasecmp(request_text, "close")) {
		// TODO: check if session->slot != 0
		JANUS_LOG(LOG_INFO, "VideoMUX: CLOSE session %llu\n", session->sdp_sessid);

		if (session->slot <= 0) {
			JANUS_LOG(LOG_WARN, "VideoMUX: CLOSE session %llu: no slot\n", session->sdp_sessid);
		} else {
			curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
			sprintf(vvurl, "%s/session/%llu", SERVICE_URL, session->sdp_sessid);
			curl_easy_setopt(curl, CURLOPT_URL, vvurl);
			res = curl_easy_perform(curl);
			curl_easy_cleanup(curl);
			session->slot = 0;
		}
		json_object_set_new(reply, "videomux", json_string("success"));
	} else if (!strcasecmp(request_text, "stop")) {

		JANUS_LOG(LOG_INFO, "VideoMUX: STOP session %llu\n", session->sdp_sessid);
		if (session->slot <= 0) {
			JANUS_LOG(LOG_WARN, "VideoMUX: STOP session %llu: no slot\n", session->sdp_sessid);
		} else {
			sprintf(vvurl, "%s/stop?id=%llu", SERVICE_URL, session->sdp_sessid);
			curl_easy_setopt(curl, CURLOPT_URL, vvurl);
			res = curl_easy_perform(curl);
			curl_easy_cleanup(curl);
		}
		json_object_set_new(reply, "videomux", json_string("success"));
	} else if(!strcasecmp(request_text, "watch")) {
		if (session->slot <= 0) {
			// TODO: return error
			JANUS_LOG(LOG_WARN, "VideoMUX: WATCH session %llu: no slot\n", session->sdp_sessid);
			error_code = JANUS_VIDEOMUX_ERROR_NO_SLOT;
			g_snprintf(error_cause, 512, "No slot reserved");
			goto msg_response;
		} else {
			JANUS_VALIDATE_JSON_OBJECT(root, watch_parameters,
				error_code, error_cause, TRUE,
				JANUS_VIDEOMUX_ERROR_MISSING_ELEMENT, JANUS_VIDEOMUX_ERROR_INVALID_ELEMENT);

			if(error_code != 0) {
				goto msg_response;
			}

			string s;
			int init_res;
  		init_res = init_string(&s);

			if (init_res >= 0) {

				json_t *cam_ref = json_object_get(root, "cam");
				json_t *hash_ref = json_object_get(root, "hash");
				json_t *sens_ref = json_object_get(root, "sens");
				json_t *time_ref = json_object_get(root, "time");

				if (cam_ref != NULL) {
					int cam_id = json_integer_value(cam_ref);
					sprintf(vvurl, "%s/view?id=%llu&cam=%d", SERVICE_URL, session->sdp_sessid, cam_id);
				} else if (hash_ref != NULL && sens_ref != NULL && time_ref != NULL) {
					const char *hash = json_string_value(hash_ref);
					const char *sens = json_string_value(sens_ref);
					const char *timev = json_string_value(time_ref);
					sprintf(vvurl, "%s/view?id=%llu&hash=%s&sens=%s&time=%s", SERVICE_URL, session->sdp_sessid, hash, sens, timev);
				} else {
					JANUS_LOG(LOG_ERR, "Watch: no parameters\n");
					error_code = JANUS_VIDEOMUX_ERROR_MISSING_ELEMENT;
					g_snprintf(error_cause, 512, "Missing elements in watch request");
					goto msg_response;
				}

				curl_easy_setopt(curl, CURLOPT_URL, vvurl);
				curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
				curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
				res = curl_easy_perform(curl);
				curl_easy_cleanup(curl);

				if(res != CURLE_OK) { // TODO: connection error
					JANUS_LOG(LOG_WARN, "Curl error\n");
					error_code = JANUS_VIDEOMUX_ERROR_CONNECTION_ERROR;
					g_snprintf(error_cause, 512, "Connection error (curl_easy_perform)");
					free(s.ptr);
					goto msg_response;
				} else {
					JANUS_LOG(LOG_INFO, "\t\tVideoMUX: GOT OPEN response: %s\n", s.ptr);

					json_error_t error;
					json_t *jresp = json_loads(s.ptr, 0, &error);
					free(s.ptr);

					if(!jresp) {
						JANUS_LOG(LOG_WARN, "Error parsing data VideoMux message (JSON error: on line %d: %s)\n", error.line, error.text);
						error_code = JANUS_VIDEOMUX_ERROR_CONNECTION_ERROR;
						g_snprintf(error_cause, 512, "Connection error (invalid JSON)");
						goto msg_response;
					}

					json_t *jerror = json_object_get(jresp, "error");
					json_t *jxaddr = json_object_get(jresp, "xaddress");

					if (jerror == NULL) {
						json_object_set_new(reply, "videomux", json_string("success"));
						if (jxaddr != NULL) {
							const char *xaddr = json_string_value(jxaddr);
							JANUS_LOG(LOG_INFO, "\t\t*** VideoMUX: GOT XADDRESS: %s\n", xaddr);
							json_object_set_new(reply, "xaddress", json_string(xaddr));
						}
					} else { // TODO: error
						const char *error_resp = json_string_value(jerror);
						JANUS_LOG(LOG_WARN, "\t\tVideoMUX: ERROR response: %s\n", error_resp);

						error_code = JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR;
						g_snprintf(error_cause, 512, "Error: %s", error_resp);
						goto msg_response;
					}
				}
			}
		}

		/* By default we send a confirmation back to the user that sent this message:
		 * if the user passed an ack=false, though, we don't do that */
		json_t *ack = json_object_get(root, "ack");
		if(!internal && (ack == NULL || json_is_true(ack))) {
			/* Send response back */
		} else {
			internal = TRUE;
			json_decref(reply);
			reply = NULL;
		}
	} else {
		JANUS_LOG(LOG_ERR, "Unsupported request %s\n", request_text);
		error_code = JANUS_VIDEOMUX_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unsupported request %s", request_text);
		goto msg_response;
	}

msg_response:
		{
			if(!internal) {
				if(error_code == 0 && !reply) {
					error_code = JANUS_VIDEOMUX_ERROR_UNKNOWN_ERROR;
					g_snprintf(error_cause, 512, "Invalid response");
				}
				if(error_code != 0) {
					/* Prepare JSON error event */
					json_t *event = json_object();
					json_object_set_new(event, "videomux", json_string("error"));
					json_object_set_new(event, "error_code", json_integer(error_code));
					json_object_set_new(event, "error", json_string(error_cause));
					reply = event;
				}
				// if(transaction_text && json == NULL)
				// 	json_object_set_new(reply, "transaction", json_string(transaction_text));
				if(json == NULL) {
					/* Reply via data channels */
					char *reply_text = json_dumps(reply, json_format);
					json_decref(reply);
					gateway->relay_data(handle, reply_text, strlen(reply_text));
					free(reply_text);
				} else {
					/* Reply via Janus API */
					return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, reply);
				}
			}

			if(root != NULL)
				json_decref(root);
		}
	return NULL;
}

void janus_videomux_slow_link(janus_plugin_session *handle, int uplink, int video) {
	/* We don't do audio/video */
}

void janus_videomux_hangup_media(janus_plugin_session *handle) {
	janus_mutex_lock(&sessions_mutex);
	janus_videomux_hangup_media_internal(handle);
	janus_mutex_unlock(&sessions_mutex);
}

static void janus_videomux_hangup_media_internal(janus_plugin_session *handle) {
	// TODO: close session
	JANUS_LOG(LOG_INFO, "No WebRTC media anymore\n");
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_videomux_session *session = janus_videomux_lookup_session(handle);
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	if(session->destroyed)
		return;
	if(g_atomic_int_add(&session->hangingup, 1))
		return;
}

/* Thread to handle incoming messages */
static void *janus_videomux_handler(void *data) {
	JANUS_LOG(LOG_VERB, "Joining VideoMux handler thread\n");
	janus_videomux_message *msg = NULL;
	int error_code = 0;
	char error_cause[512];
	json_t *root = NULL;
	gboolean do_offer = FALSE, sdp_update = FALSE;
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		msg = g_async_queue_pop(messages);
		if(msg == NULL)
			continue;
		if(msg == &exit_message)
			break;
		if(msg->handle == NULL) {
			janus_videomux_message_free(msg);
			continue;
		}
		janus_mutex_lock(&sessions_mutex);
		janus_videomux_session *session = janus_videomux_lookup_session(msg->handle);
		if(!session) {
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			janus_videomux_message_free(msg);
			continue;
		}
		if(session->destroyed) {
			janus_mutex_unlock(&sessions_mutex);
			janus_videomux_message_free(msg);
			continue;
		}
		janus_mutex_unlock(&sessions_mutex);
		/* Handle request */
		error_code = 0;
		root = msg->message;
		if(msg->message == NULL) {
			JANUS_LOG(LOG_ERR, "No message??\n");
			error_code = JANUS_VIDEOMUX_ERROR_NO_MESSAGE;
			g_snprintf(error_cause, 512, "%s", "No message??");
			goto error;
		}
		if(!json_is_object(root)) {
			JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
			error_code = JANUS_VIDEOMUX_ERROR_INVALID_JSON;
			g_snprintf(error_cause, 512, "JSON error: not an object");
			goto error;
		}
		/* Parse request */
		JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
			error_code, error_cause, TRUE,
			JANUS_VIDEOMUX_ERROR_MISSING_ELEMENT, JANUS_VIDEOMUX_ERROR_INVALID_ELEMENT);
		if(error_code != 0)
			goto error;
		do_offer = FALSE;
		sdp_update = FALSE;
		json_t *request = json_object_get(root, "request");
		const char *request_text = json_string_value(request);
		do_offer = FALSE;
		if(!strcasecmp(request_text, "setup")) {
			if(!g_atomic_int_compare_and_exchange(&session->setup, 0, 1)) {
				JANUS_LOG(LOG_ERR, "PeerConnection already setup\n");
				error_code = JANUS_VIDEOMUX_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection already setup");
				goto error;
			}
			do_offer = TRUE;
		} else if(!strcasecmp(request_text, "restart")) {
			if(!g_atomic_int_get(&session->setup)) {
				JANUS_LOG(LOG_ERR, "PeerConnection not setup\n");
				error_code = JANUS_VIDEOMUX_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection not setup");
				goto error;
			}
			sdp_update = TRUE;
			do_offer = TRUE;
		} else if(!strcasecmp(request_text, "ack")) {
			/* The peer sent their answer back: do nothing */
		} else {
			JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
			error_code = JANUS_VIDEOMUX_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
			goto error;
		}

		/* Prepare JSON event */
		json_t *event = json_object();
		json_object_set_new(event, "videomux", json_string("event"));
		json_object_set_new(event, "result", json_string("ok"));
		if(!do_offer) {
			int ret = gateway->push_event(msg->handle, &janus_videomux_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
		} else {
			/* Send an offer (whether it's for an ICE restart or not) */
			if(sdp_update) {
				/* Renegotiation: increase version */
				session->sdp_version++;
			} else {
				/* New session: generate new values */
				session->sdp_version = 1;	/* This needs to be increased when it changes */
				session->sdp_sessid = janus_get_real_time();
			}
			char sdp[500];
			g_snprintf(sdp, sizeof(sdp), sdp_template,
				session->sdp_sessid, session->sdp_version);
			json_t *jsep = json_pack("{ssss}", "type", "offer", "sdp", sdp);
			if(sdp_update)
				json_object_set_new(jsep, "restart", json_true());
			/* How long will the gateway take to push the event? */
			g_atomic_int_set(&session->hangingup, 0);
			gint64 start = janus_get_monotonic_time();
			int res = gateway->push_event(msg->handle, &janus_videomux_plugin, msg->transaction, event, jsep);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (took %"SCNu64" us)\n",
				res, janus_get_monotonic_time()-start);
			json_decref(jsep);
		}
		json_decref(event);
		janus_videomux_message_free(msg);
		continue;

error:
		{
			/* Prepare JSON error event */
			json_t *event = json_object();
			json_object_set_new(event, "videomux", json_string("error"));
			json_object_set_new(event, "error_code", json_integer(error_code));
			json_object_set_new(event, "error", json_string(error_cause));
			int ret = gateway->push_event(msg->handle, &janus_videomux_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
			json_decref(event);
			janus_videomux_message_free(msg);
		}
	}
	JANUS_LOG(LOG_VERB, "Leaving VideoMux handler thread\n");
	return NULL;
}
