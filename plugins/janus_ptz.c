/*! \file   janus_ptz.c
 * \author
 * \copyright
 * \brief  Janus PTZ plugin
 * \details This is a plugin implementing a DataChannel only PTZ control.
 *
 * The only message that is typically sent to the plugin through the Janus API is
 * a "setup" message, by which the user initializes the PeerConnection
 * itself. Apart from that, all other messages can be exchanged directly
 * via Data Channels.
 *
 * \section ptzapi PTZ API
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
#define JANUS_PTZ_VERSION			2
#define JANUS_PTZ_VERSION_STRING	"0.0.2"
#define JANUS_PTZ_DESCRIPTION		"This is a plugin implementing a PTZ control for Janus, using DataChannels."
#define JANUS_PTZ_NAME				"JANUS PTZ plugin"
#define JANUS_PTZ_AUTHOR			"ISMB <bertone@imsb.it>"
#define JANUS_PTZ_PACKAGE			"janus.plugin.ptz"

/* Plugin methods */
janus_plugin *create(void);
int janus_ptz_init(janus_callbacks *callback, const char *config_path);
void janus_ptz_destroy(void);
int janus_ptz_get_api_compatibility(void);
int janus_ptz_get_version(void);
const char *janus_ptz_get_version_string(void);
const char *janus_ptz_get_description(void);
const char *janus_ptz_get_name(void);
const char *janus_ptz_get_author(void);
const char *janus_ptz_get_package(void);
void janus_ptz_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_ptz_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_ptz_setup_media(janus_plugin_session *handle);
void janus_ptz_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_ptz_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_ptz_incoming_data(janus_plugin_session *handle, char *buf, int len);
void janus_ptz_slow_link(janus_plugin_session *handle, int uplink, int video, int nacks);
void janus_ptz_hangup_media(janus_plugin_session *handle);
void janus_ptz_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_ptz_query_session(janus_plugin_session *handle);

/* Plugin setup */
static janus_plugin janus_ptz_plugin =
	JANUS_PLUGIN_INIT (
		.init = janus_ptz_init,
		.destroy = janus_ptz_destroy,
		.get_api_compatibility = janus_ptz_get_api_compatibility,
		.get_version = janus_ptz_get_version,
		.get_version_string = janus_ptz_get_version_string,
		.get_description = janus_ptz_get_description,
		.get_name = janus_ptz_get_name,
		.get_author = janus_ptz_get_author,
		.get_package = janus_ptz_get_package,
		.create_session = janus_ptz_create_session,
		.handle_message = janus_ptz_handle_message,
		.setup_media = janus_ptz_setup_media,
		.incoming_rtp = janus_ptz_incoming_rtp,
		.incoming_rtcp = janus_ptz_incoming_rtcp,
		.incoming_data = janus_ptz_incoming_data,
		.slow_link = janus_ptz_slow_link,
		.hangup_media = janus_ptz_hangup_media,
		.destroy_session = janus_ptz_destroy_session,
		.query_session = janus_ptz_query_session,
	);

/* Plugin creator */
janus_plugin *create(void) {
	JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_PTZ_NAME);
	return &janus_ptz_plugin;
}

/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

static struct janus_json_parameter transaction_parameters[] = {
	{"ptz", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
	{"transaction", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

static struct janus_json_parameter message_parameters[] = {
	{"cam", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
	{"command", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
	{"ack", JANUS_JSON_BOOL, 0}
};

/* Useful stuff */
static volatile gint initialized = 0, stopping = 0;
// static gboolean notify_events = TRUE;
static janus_callbacks *gateway = NULL;
static GThread *handler_thread;
static GThread *watchdog;
static void *janus_ptz_handler(void *data);
static void janus_ptz_hangup_media_internal(janus_plugin_session *handle);

/* JSON serialization options */
static size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;

typedef struct janus_ptz_message {
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;
} janus_ptz_message;

static GAsyncQueue *messages = NULL;
static janus_ptz_message exit_message;

static void janus_ptz_message_free(janus_ptz_message *msg) {
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

typedef struct janus_ptz_session {
	janus_plugin_session *handle;
	gint64 sdp_sessid;
	gint64 sdp_version;
	janus_mutex mutex;			/* Mutex to lock this session */
	volatile gint setup;
	volatile gint hangingup;
	gint64 destroyed;	/* Time at which this session was marked as destroyed */
} janus_ptz_session;

static GHashTable *sessions;
static GList *old_sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;

/* SDP template: we only offer data channels */
#define sdp_template \
		"v=0\r\n" \
		"o=- %"SCNu64" %"SCNu64" IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
		"s=Janus PTZ plugin\r\n" \
		"t=0 0\r\n" \
		"m=application 1 DTLS/SCTP 5000\r\n" \
		"c=IN IP4 1.1.1.1\r\n" \
		"a=sctpmap:5000 webrtc-datachannel 16\r\n"

/* Error codes */
#define JANUS_PTZ_ERROR_NO_MESSAGE			411
#define JANUS_PTZ_ERROR_INVALID_JSON		412
#define JANUS_PTZ_ERROR_MISSING_ELEMENT	413
#define JANUS_PTZ_ERROR_INVALID_ELEMENT	414
#define JANUS_PTZ_ERROR_INVALID_REQUEST	415
#define JANUS_PTZ_ERROR_ALREADY_SETUP		416
#define JANUS_PTZ_ERROR_NO_SUCH_CAM			423
#define JANUS_PTZ_ERROR_UNKNOWN_ERROR		499

#define SERVICE_URL "http://127.0.0.1:2087"

/* CURL handler */
CURL *curl;

/* PTZ watchdog/garbage collector (sort of) */
static void *janus_ptz_watchdog(void *data) {
	JANUS_LOG(LOG_INFO, "PTZ watchdog started\n");
	gint64 now = 0;
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		janus_mutex_lock(&sessions_mutex);
		/* Iterate on all the sessions */
		now = janus_get_monotonic_time();
		if(old_sessions != NULL) {
			GList *sl = old_sessions;
			JANUS_LOG(LOG_HUGE, "Checking %d old PTZ sessions...\n", g_list_length(old_sessions));
			while(sl) {
				janus_ptz_session *session = (janus_ptz_session *)sl->data;
				if(!session) {
					sl = sl->next;
					continue;
				}
				if(now-session->destroyed >= 5*G_USEC_PER_SEC) {
					/* We're lazy and actually get rid of the stuff only after a few seconds */
					JANUS_LOG(LOG_VERB, "Freeing old PTZ session\n");
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
	JANUS_LOG(LOG_INFO, "PTZ watchdog stopped\n");
	return NULL;
}

/* We use this method to handle incoming requests. Since most of the requests
 * will arrive from data channels, but some may also arrive from the regular
 * plugin messaging (e.g., room management), we have the ability to pass
 * parsed JSON objects instead of strings, which explains why we specify a
 * janus_plugin_result pointer as a return value; messages handles via
 * datachannels would simply return NULL. Besides, some requests are actually
 * originated internally, and don't need any response to be sent to anyone,
 * which is what the additional boolean "internal" value is for */
janus_plugin_result *janus_ptz_handle_incoming_request(janus_plugin_session *handle,
	char *text, json_t *json, gboolean internal);

/* Plugin implementation */
int janus_ptz_init(janus_callbacks *callback, const char *config_path) {
	if(g_atomic_int_get(&stopping)) {
		/* Still stopping from before */
		return -1;
	}
	if(callback == NULL) {
		/* Invalid arguments */
		return -1;
	}

	sessions = g_hash_table_new(NULL, NULL);
	messages = g_async_queue_new_full((GDestroyNotify) janus_ptz_message_free);
	/* This is the callback we'll need to invoke to contact the gateway */
	gateway = callback;

	curl_global_init(CURL_GLOBAL_ALL);
	curl = curl_easy_init();
	if(curl == NULL) {
		JANUS_LOG(LOG_ERR, "Can't init CURL\n");
		return -1;
	}

	g_atomic_int_set(&initialized, 1);

	GError *error = NULL;
	/* Start the sessions watchdog */
	watchdog = g_thread_try_new("PTZ watchdog", &janus_ptz_watchdog, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the PTZ watchdog thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}

	/* Launch the thread that will handle incoming messages */
	handler_thread = g_thread_try_new("PTZ handler", janus_ptz_handler, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the PTZ handler thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}
	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_PTZ_NAME);
	return 0;
}

void janus_ptz_destroy(void) {
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

	curl_easy_cleanup(curl);
	curl_global_cleanup();

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);
	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_PTZ_NAME);
}

int janus_ptz_get_api_compatibility(void) {
	/* Important! This is what your plugin MUST always return: don't lie here or bad things will happen */
	return JANUS_PLUGIN_API_VERSION;
}

int janus_ptz_get_version(void) {
	return JANUS_PTZ_VERSION;
}

const char *janus_ptz_get_version_string(void) {
	return JANUS_PTZ_VERSION_STRING;
}

const char *janus_ptz_get_description(void) {
	return JANUS_PTZ_DESCRIPTION;
}

const char *janus_ptz_get_name(void) {
	return JANUS_PTZ_NAME;
}

const char *janus_ptz_get_author(void) {
	return JANUS_PTZ_AUTHOR;
}

const char *janus_ptz_get_package(void) {
	return JANUS_PTZ_PACKAGE;
}

static janus_ptz_session *janus_ptz_lookup_session(janus_plugin_session *handle) {
	janus_ptz_session *session = NULL;
	if (g_hash_table_contains(sessions, handle)) {
		session = (janus_ptz_session *)handle->plugin_handle;
	}
	return session;
}

void janus_ptz_create_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_ptz_session *session = (janus_ptz_session *)g_malloc0(sizeof(janus_ptz_session));
	session->handle = handle;
	session->destroyed = 0;
	janus_mutex_init(&session->mutex);
	g_atomic_int_set(&session->setup, 0);
	g_atomic_int_set(&session->hangingup, 0);
	handle->plugin_handle = session;
	janus_mutex_lock(&sessions_mutex);
	g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

void janus_ptz_destroy_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_ptz_session *session = janus_ptz_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		*error = -2;
		return;
	}
	if(!session->destroyed) {
		JANUS_LOG(LOG_VERB, "Removing PTZ session...\n");
		janus_ptz_hangup_media_internal(handle);
		session->destroyed = janus_get_monotonic_time();
		g_hash_table_remove(sessions, handle);
		/* Cleaning up and removing the session is done in a lazy way */
		old_sessions = g_list_append(old_sessions, session);
	}
	janus_mutex_unlock(&sessions_mutex);
	return;
}

json_t *janus_ptz_query_session(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		return NULL;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_ptz_session *session = janus_ptz_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return NULL;
	}

	json_t *info = json_object();
	json_object_set_new(info, "destroyed", json_integer(session->destroyed));
	janus_mutex_unlock(&sessions_mutex);
	return info;
}

/* messages received through API (no datachannel) */
struct janus_plugin_result *janus_ptz_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
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
		error_code = JANUS_PTZ_ERROR_NO_MESSAGE;
		g_snprintf(error_cause, 512, "%s", "No message??");
		goto plugin_response;
	}

	janus_ptz_session *session = janus_ptz_lookup_session(handle);
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		error_code = JANUS_PTZ_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "session associated with this handle...");
		goto plugin_response;
	}
	if(session->destroyed) {
		JANUS_LOG(LOG_ERR, "Session has already been destroyed...\n");
		error_code = JANUS_PTZ_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "Session has already been destroyed...");
		goto plugin_response;
	}
	if(!json_is_object(root)) {
		JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
		error_code = JANUS_PTZ_ERROR_INVALID_JSON;
		g_snprintf(error_cause, 512, "JSON error: not an object");
		goto plugin_response;
	}
	/* Get the request first */
	JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
		error_code, error_cause, TRUE,
		JANUS_PTZ_ERROR_MISSING_ELEMENT, JANUS_PTZ_ERROR_INVALID_ELEMENT);
	if(error_code != 0)
		goto plugin_response;
	json_t *request = json_object_get(root, "request");

	const char *request_text = json_string_value(request);

	// if(!strcasecmp(request_text, "list")
	// 		|| !strcasecmp(request_text, "exists")
	// 		|| !strcasecmp(request_text, "create")
	// 		|| !strcasecmp(request_text, "edit")
	// 		|| !strcasecmp(request_text, "destroy")) {
	// 	/* These requests typically only belong to the datachannel
	// 	 * messaging, but for admin purposes we might use them on
	// 	 * the Janus API as well: add the properties the datachannel
	// 	 * processor would expect and handle everything there */
	// 	json_object_set_new(root, "ptz", json_string(request_text));
	// 	json_object_set_new(root, "transaction", json_string(transaction));
	// 	janus_plugin_result *result = janus_ptz_handle_incoming_request(session->handle, NULL, root, FALSE);
	// 	if(result == NULL) {
	// 		JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
	// 		error_code = JANUS_PTZ_ERROR_INVALID_JSON;
	// 		g_snprintf(error_cause, 512, "JSON error: not an object");
	// 		goto plugin_response;
	// 	}
	// 	janus_mutex_unlock(&sessions_mutex);
	// 	if(root != NULL)
	// 		json_decref(root);
	// 	if(jsep != NULL)
	// 		json_decref(jsep);
	// 	g_free(transaction);
	// 	return result;
	// } else

	if(!strcasecmp(request_text, "setup") || !strcasecmp(request_text, "ack") || !strcasecmp(request_text, "restart")) {
		/* These messages are handled asynchronously */
		janus_mutex_unlock(&sessions_mutex);
		janus_ptz_message *msg = g_malloc0(sizeof(janus_ptz_message));
		msg->handle = handle;
		msg->transaction = transaction;
		msg->message = root;
		msg->jsep = jsep;

		g_async_queue_push(messages, msg);

		return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
	} else {
		JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
		error_code = JANUS_PTZ_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
	}

plugin_response:
		{
			janus_mutex_unlock(&sessions_mutex);
			if(!response) {
				/* Prepare JSON error event */
				response = json_object();
				json_object_set_new(response, "ptz", json_string("event"));
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

void janus_ptz_setup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "WebRTC media is now available\n");
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_mutex_lock(&sessions_mutex);
	janus_ptz_session *session = janus_ptz_lookup_session(handle);
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

void janus_ptz_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len) {
	/* We don't do audio/video */
}

void janus_ptz_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len) {
	/* We don't do audio/video */
}

/* messages received through DataChannel */
void janus_ptz_incoming_data(janus_plugin_session *handle, char *buf, int len) {
	if(handle == NULL || handle->stopped || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	/* Incoming request from this user: what should we do? */
	janus_ptz_session *session = (janus_ptz_session *)handle->plugin_handle;
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
	JANUS_LOG(LOG_VERB, "Got a DataChannel message (%zu bytes): %s\n", strlen(text), text);
	janus_ptz_handle_incoming_request(handle, text, NULL, FALSE);
}

janus_plugin_result *janus_ptz_handle_incoming_request(janus_plugin_session *handle, char *text, json_t *json, gboolean internal) {
	CURLcode res;
	char ptzurl[128];
	char urnfield[128];

	/* Parse JSON, if needed */
	json_error_t error;
	json_t *root = text ? json_loads(text, 0, &error) : json;
	g_free(text);
	if(!root) {
		JANUS_LOG(LOG_ERR, "Error parsing data channel message (JSON error: on line %d: %s)\n", error.line, error.text);
		return NULL;
	}
	/* Handle request */
	int error_code = 0;
	char error_cause[512];
	JANUS_VALIDATE_JSON_OBJECT(root, transaction_parameters,
		error_code, error_cause, TRUE,
		JANUS_PTZ_ERROR_MISSING_ELEMENT, JANUS_PTZ_ERROR_INVALID_ELEMENT);
	const char *transaction_text = NULL;
	json_t *reply = NULL;
	if(error_code != 0)
		goto msg_response;
	json_t *request = json_object_get(root, "ptz");
	json_t *transaction = json_object_get(root, "transaction");
	const char *request_text = json_string_value(request);
	transaction_text = json_string_value(transaction);
	if(!strcasecmp(request_text, "message")) {
		JANUS_VALIDATE_JSON_OBJECT(root, message_parameters,
			error_code, error_cause, TRUE,
			JANUS_PTZ_ERROR_MISSING_ELEMENT, JANUS_PTZ_ERROR_INVALID_ELEMENT);
		if(error_code != 0)
			goto msg_response;
		json_t *cam_ref = json_object_get(root, "cam");
		const char *cam_id = json_string_value(cam_ref);

		json_t *text = json_object_get(root, "command");
		const char *message = json_string_value(text);

    if(curl) {
      if (!strcasecmp(message, "up")) {
        JANUS_LOG(LOG_VERB, "UP!\n");
        sprintf(ptzurl, "%s/up", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      } else if (!strcasecmp(message, "down")) {
        JANUS_LOG(LOG_VERB, "DOWN!\n");
        sprintf(ptzurl, "%s/down", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      } else if (!strcasecmp(message, "left")) {
        JANUS_LOG(LOG_VERB, "LEFT!\n");
        sprintf(ptzurl, "%s/left", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      } else if (!strcasecmp(message, "right")) {
        JANUS_LOG(LOG_VERB, "RIGHT!\n");
        sprintf(ptzurl, "%s/right", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      } else if (!strcasecmp(message, "zoom_plus")) {
        JANUS_LOG(LOG_VERB, "zoom_plus!\n");
        sprintf(ptzurl, "%s/zoom_plus", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      } else if (!strcasecmp(message, "zoom_minus")) {
        JANUS_LOG(LOG_VERB, "zoom_minus!\n");
        sprintf(ptzurl, "%s/zoom_minus", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      } else {
        JANUS_LOG(LOG_VERB, "STOP!\n");
        sprintf(ptzurl, "%s/stop", SERVICE_URL);
        curl_easy_setopt(curl, CURLOPT_URL, ptzurl);
      }

	    // sprintf(urnfield, "urn=%s", cam_id);
	    // JANUS_LOG(LOG_VERB, "urn: %s\n", urnfield);

			sprintf(urnfield, "xaddress=%s", cam_id);
			JANUS_LOG(LOG_VERB, "xaddress: %s\n", urnfield);

	    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, urnfield); // "urn=urn:uuid:812b12f7-45a0-11b5-8404-c056e3a2e020");

	    /* Perform the request, res will get the return code */
	    res = curl_easy_perform(curl);
	    /* Check for errors */
	    if(res != CURLE_OK)
	      fprintf(stderr, "curl_easy_perform() failed: %s\n",
	              curl_easy_strerror(res));

		}

		/* Start preparing the response too */
		reply = json_object();
		json_object_set_new(reply, "ptz", json_string("success"));

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
		error_code = JANUS_PTZ_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unsupported request %s", request_text);
		goto msg_response;
	}

msg_response:
		{
			if(!internal) {
				if(error_code == 0 && !reply) {
					error_code = JANUS_PTZ_ERROR_UNKNOWN_ERROR;
					g_snprintf(error_cause, 512, "Invalid response");
				}
				if(error_code != 0) {
					/* Prepare JSON error event */
					json_t *event = json_object();
					json_object_set_new(event, "ptz", json_string("error"));
					json_object_set_new(event, "error_code", json_integer(error_code));
					json_object_set_new(event, "error", json_string(error_cause));
					reply = event;
				}
				if(transaction_text && json == NULL)
					json_object_set_new(reply, "transaction", json_string(transaction_text));
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

void janus_ptz_slow_link(janus_plugin_session *handle, int uplink, int video, int nacks) {
	/* We don't do audio/video */
}

void janus_ptz_hangup_media(janus_plugin_session *handle) {
	janus_mutex_lock(&sessions_mutex);
	janus_ptz_hangup_media_internal(handle);
	janus_mutex_unlock(&sessions_mutex);
}

static void janus_ptz_hangup_media_internal(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "No WebRTC media anymore\n");
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_ptz_session *session = janus_ptz_lookup_session(handle);
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
static void *janus_ptz_handler(void *data) {
	JANUS_LOG(LOG_VERB, "Joining PTZ handler thread\n");
	janus_ptz_message *msg = NULL;
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
			janus_ptz_message_free(msg);
			continue;
		}
		janus_mutex_lock(&sessions_mutex);
		janus_ptz_session *session = janus_ptz_lookup_session(msg->handle);
		if(!session) {
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			janus_ptz_message_free(msg);
			continue;
		}
		if(session->destroyed) {
			janus_mutex_unlock(&sessions_mutex);
			janus_ptz_message_free(msg);
			continue;
		}
		janus_mutex_unlock(&sessions_mutex);
		/* Handle request */
		error_code = 0;
		root = msg->message;
		if(msg->message == NULL) {
			JANUS_LOG(LOG_ERR, "No message??\n");
			error_code = JANUS_PTZ_ERROR_NO_MESSAGE;
			g_snprintf(error_cause, 512, "%s", "No message??");
			goto error;
		}
		if(!json_is_object(root)) {
			JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
			error_code = JANUS_PTZ_ERROR_INVALID_JSON;
			g_snprintf(error_cause, 512, "JSON error: not an object");
			goto error;
		}
		/* Parse request */
		JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
			error_code, error_cause, TRUE,
			JANUS_PTZ_ERROR_MISSING_ELEMENT, JANUS_PTZ_ERROR_INVALID_ELEMENT);
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
				error_code = JANUS_PTZ_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection already setup");
				goto error;
			}
			do_offer = TRUE;
		} else if(!strcasecmp(request_text, "restart")) {
			if(!g_atomic_int_get(&session->setup)) {
				JANUS_LOG(LOG_ERR, "PeerConnection not setup\n");
				error_code = JANUS_PTZ_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection not setup");
				goto error;
			}
			sdp_update = TRUE;
			do_offer = TRUE;
		} else if(!strcasecmp(request_text, "ack")) {
			/* The peer sent their answer back: do nothing */
		} else {
			JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
			error_code = JANUS_PTZ_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
			goto error;
		}

		/* Prepare JSON event */
		json_t *event = json_object();
		json_object_set_new(event, "ptz", json_string("event"));
		json_object_set_new(event, "result", json_string("ok"));
		if(!do_offer) {
			int ret = gateway->push_event(msg->handle, &janus_ptz_plugin, msg->transaction, event, NULL);
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
			int res = gateway->push_event(msg->handle, &janus_ptz_plugin, msg->transaction, event, jsep);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (took %"SCNu64" us)\n",
				res, janus_get_monotonic_time()-start);
			json_decref(jsep);
		}
		json_decref(event);
		janus_ptz_message_free(msg);
		continue;

error:
		{
			/* Prepare JSON error event */
			json_t *event = json_object();
			json_object_set_new(event, "ptz", json_string("error"));
			json_object_set_new(event, "error_code", json_integer(error_code));
			json_object_set_new(event, "error", json_string(error_cause));
			int ret = gateway->push_event(msg->handle, &janus_ptz_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
			json_decref(event);
			janus_ptz_message_free(msg);
		}
	}
	JANUS_LOG(LOG_VERB, "Leaving PTZ handler thread\n");
	return NULL;
}
