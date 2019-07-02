/*! \file   janus_frame.c
 * \author
 * \copyright
 * \brief  Janus frame plugin
 * \details This is a plugin implementing a DataChannel only frame control.
 *
 * The only message that is typically sent to the plugin through the Janus API is
 * a "setup" message, by which the user initializes the PeerConnection
 * itself. Apart from that, all other messages can be exchanged directly
 * via Data Channels.
 *
 * \section frameapi frame API
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
#define JANUS_FRAME_VERSION			2
#define JANUS_FRAME_VERSION_STRING	"0.0.2"
#define JANUS_FRAME_DESCRIPTION		"This is a plugin implementing frame request for Janus, using DataChannels."
#define JANUS_FRAME_NAME				"JANUS frame plugin"
#define JANUS_FRAME_AUTHOR			"Saet IS <giancarlo.capella@saet.org>"
#define JANUS_FRAME_PACKAGE			"janus.plugin.frame"

/* Plugin methods */
janus_plugin *create(void);
int janus_frame_init(janus_callbacks *callback, const char *config_path);
void janus_frame_destroy(void);
int janus_frame_get_api_compatibility(void);
int janus_frame_get_version(void);
const char *janus_frame_get_version_string(void);
const char *janus_frame_get_description(void);
const char *janus_frame_get_name(void);
const char *janus_frame_get_author(void);
const char *janus_frame_get_package(void);
void janus_frame_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_frame_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_frame_setup_media(janus_plugin_session *handle);
void janus_frame_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_frame_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_frame_incoming_data(janus_plugin_session *handle, char *buf, int len);
void janus_frame_slow_link(janus_plugin_session *handle, int uplink, int video);
void janus_frame_hangup_media(janus_plugin_session *handle);
void janus_frame_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_frame_query_session(janus_plugin_session *handle);
void janus_frame_queue_request(janus_plugin_session *handle, int cam, int hd);
void janus_frame_stop(janus_plugin_session *handle);

/* Plugin setup */
static janus_plugin janus_frame_plugin =
	JANUS_PLUGIN_INIT (
		.init = janus_frame_init,
		.destroy = janus_frame_destroy,
		.get_api_compatibility = janus_frame_get_api_compatibility,
		.get_version = janus_frame_get_version,
		.get_version_string = janus_frame_get_version_string,
		.get_description = janus_frame_get_description,
		.get_name = janus_frame_get_name,
		.get_author = janus_frame_get_author,
		.get_package = janus_frame_get_package,
		.create_session = janus_frame_create_session,
		.handle_message = janus_frame_handle_message,
		.setup_media = janus_frame_setup_media,
		.incoming_rtp = janus_frame_incoming_rtp,
		.incoming_rtcp = janus_frame_incoming_rtcp,
		.incoming_data = janus_frame_incoming_data,
		.slow_link = janus_frame_slow_link,
		.hangup_media = janus_frame_hangup_media,
		.destroy_session = janus_frame_destroy_session,
		.query_session = janus_frame_query_session,
	);

/* Plugin creator */
janus_plugin *create(void) {
	JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_FRAME_NAME);
	return &janus_frame_plugin;
}

/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

#if 0
static struct janus_json_parameter transaction_parameters[] = {
	{"frame", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
	//{"transaction", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
#endif

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
static void *janus_frame_handler(void *data);
static void janus_frame_hangup_media_internal(janus_plugin_session *handle);

/* JSON serialization options */
static size_t json_format = JSON_INDENT(3) | JSON_PRESERVE_ORDER;

typedef struct janus_frame_message {
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;
} janus_frame_message;

static GAsyncQueue *messages = NULL;
static janus_frame_message exit_message;

struct {
  janus_mutex mutex;
  unsigned int request;
  unsigned int waitkf;
  unsigned int ready;
  char *data;
  int datalen;
  GThread *thread;
} janus_frames[8+1][2] = {{{0, }, 0, 0, NULL, 0, NULL}, };

janus_plugin_session *janus_handle_session[32];

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

static void janus_frame_message_free(janus_frame_message *msg) {
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

typedef struct janus_frame_session {
	janus_plugin_session *handle;
	gint64 sdp_sessid;
	gint64 sdp_version;
	janus_mutex mutex;			/* Mutex to lock this session */
	volatile gint setup;
	volatile gint hangingup;
	gint64 destroyed;	/* Time at which this session was marked as destroyed */
	
	int id;
} janus_frame_session;

static GHashTable *sessions;
static GList *old_sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;
static int session_id_seq = 0;

static janus_mutex data_mutex = JANUS_MUTEX_INITIALIZER;
struct {
	janus_frame_session *session;
	int cam, hd;
} janus_frames_data;

/* SDP template: we only offer data channels */
#define sdp_template \
		"v=0\r\n" \
		"o=- %"SCNu64" %"SCNu64" IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
		"s=Janus frame plugin\r\n" \
		"t=0 0\r\n" \
		"m=application 1 DTLS/SCTP 5000\r\n" \
		"c=IN IP4 1.1.1.1\r\n" \
		"a=sctpmap:5000 webrtc-datachannel 262144\r\n"

/* Error codes */
#define JANUS_FRAME_ERROR_NO_MESSAGE			411
#define JANUS_FRAME_ERROR_INVALID_JSON		412
#define JANUS_FRAME_ERROR_MISSING_ELEMENT	413
#define JANUS_FRAME_ERROR_INVALID_ELEMENT	414
#define JANUS_FRAME_ERROR_INVALID_REQUEST	415
#define JANUS_FRAME_ERROR_ALREADY_SETUP		416
#define JANUS_FRAME_ERROR_NO_SUCH_CAM			423
#define JANUS_FRAME_ERROR_CONNECTION_ERROR			426
#define JANUS_FRAME_ERROR_UNKNOWN_ERROR		499

#define SERVICE_URL "http://127.0.0.1:1984"

/* Frame watchdog/garbage collector (sort of) */
static void *janus_frame_watchdog(void *data) {
	JANUS_LOG(LOG_INFO, "Frame watchdog started\n");
	gint64 now = 0;
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		janus_mutex_lock(&sessions_mutex);
		/* Iterate on all the sessions */
		now = janus_get_monotonic_time();
		if(old_sessions != NULL) {
			GList *sl = old_sessions;
			JANUS_LOG(LOG_HUGE, "Checking %d old frame sessions...\n", g_list_length(old_sessions));
			while(sl) {
				janus_frame_session *session = (janus_frame_session *)sl->data;
				if(!session) {
					sl = sl->next;
					continue;
				}
				if(now-session->destroyed >= 5*G_USEC_PER_SEC) {
					/* We're lazy and actually get rid of the stuff only after a few seconds */
					JANUS_LOG(LOG_VERB, "Freeing old frame session\n");
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
	JANUS_LOG(LOG_INFO, "Frame watchdog stopped\n");
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
janus_plugin_result *janus_frame_handle_incoming_request(janus_plugin_session *handle,
	char *text, json_t *json, gboolean internal);

/* Plugin implementation */
int janus_frame_init(janus_callbacks *callback, const char *config_path) {
	if(g_atomic_int_get(&stopping)) {
		/* Still stopping from before */
		return -1;
	}
	if(callback == NULL) {
		/* Invalid arguments */
		return -1;
	}

	sessions = g_hash_table_new(NULL, NULL);
	messages = g_async_queue_new_full((GDestroyNotify) janus_frame_message_free);
	/* This is the callback we'll need to invoke to contact the gateway */
	gateway = callback;

	curl_global_init(CURL_GLOBAL_ALL);

	g_atomic_int_set(&initialized, 1);

	GError *error = NULL;
	/* Start the sessions watchdog */
	watchdog = g_thread_try_new("Frame watchdog", &janus_frame_watchdog, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the frame watchdog thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}

	/* Launch the thread that will handle incoming messages */
	handler_thread = g_thread_try_new("Frame handler", janus_frame_handler, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the frame handler thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}
	
	int i;
	for(i=0; i<(8+1); i++)
	{
		janus_mutex_init(&janus_frames[i][0].mutex);
		janus_mutex_init(&janus_frames[i][1].mutex);
	}
	
	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_FRAME_NAME);
	return 0;
}

void janus_frame_destroy(void) {
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

	curl_global_cleanup();

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);
	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_FRAME_NAME);
}

int janus_frame_get_api_compatibility(void) {
	/* Important! This is what your plugin MUST always return: don't lie here or bad things will happen */
	return JANUS_PLUGIN_API_VERSION;
}

int janus_frame_get_version(void) {
	return JANUS_FRAME_VERSION;
}

const char *janus_frame_get_version_string(void) {
	return JANUS_FRAME_VERSION_STRING;
}

const char *janus_frame_get_description(void) {
	return JANUS_FRAME_DESCRIPTION;
}

const char *janus_frame_get_name(void) {
	return JANUS_FRAME_NAME;
}

const char *janus_frame_get_author(void) {
	return JANUS_FRAME_AUTHOR;
}

const char *janus_frame_get_package(void) {
	return JANUS_FRAME_PACKAGE;
}

static janus_frame_session *janus_frame_lookup_session(janus_plugin_session *handle) {
	janus_frame_session *session = NULL;
	if (g_hash_table_contains(sessions, handle)) {
		session = (janus_frame_session *)handle->plugin_handle;
	}
	return session;
}

void janus_frame_create_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_frame_session *session = (janus_frame_session *)g_malloc0(sizeof(janus_frame_session));
	session->handle = handle;
	session->destroyed = 0;
	janus_mutex_init(&session->mutex);
	g_atomic_int_set(&session->setup, 0);
	g_atomic_int_set(&session->hangingup, 0);
	handle->plugin_handle = session;
	janus_mutex_lock(&sessions_mutex);
	session->id = session_id_seq++;
	session_id_seq &= 31;
	g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

void janus_frame_destroy_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_frame_session *session = janus_frame_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		*error = -2;
		return;
	}
	if(!session->destroyed) {
		JANUS_LOG(LOG_VERB, "Removing frame session...\n");
		janus_frame_hangup_media_internal(handle);
		session->destroyed = janus_get_monotonic_time();
		g_hash_table_remove(sessions, handle);
		/* Cleaning up and removing the session is done in a lazy way */
		old_sessions = g_list_append(old_sessions, session);
	}
	janus_mutex_unlock(&sessions_mutex);
	
	janus_frame_stop(handle);
	return;
}

json_t *janus_frame_query_session(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		return NULL;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_frame_session *session = janus_frame_lookup_session(handle);
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
struct janus_plugin_result *janus_frame_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
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
		error_code = JANUS_FRAME_ERROR_NO_MESSAGE;
		g_snprintf(error_cause, 512, "%s", "No message??");
		goto plugin_response;
	}

	janus_frame_session *session = janus_frame_lookup_session(handle);
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		error_code = JANUS_FRAME_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "session associated with this handle...");
		goto plugin_response;
	}
	if(session->destroyed) {
		JANUS_LOG(LOG_ERR, "Session has already been destroyed...\n");
		error_code = JANUS_FRAME_ERROR_UNKNOWN_ERROR;
		g_snprintf(error_cause, 512, "%s", "Session has already been destroyed...");
		goto plugin_response;
	}
	if(!json_is_object(root)) {
		JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
		error_code = JANUS_FRAME_ERROR_INVALID_JSON;
		g_snprintf(error_cause, 512, "JSON error: not an object");
		goto plugin_response;
	}
	/* Get the request first */
	JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
		error_code, error_cause, TRUE,
		JANUS_FRAME_ERROR_MISSING_ELEMENT, JANUS_FRAME_ERROR_INVALID_ELEMENT);
	if(error_code != 0)
		goto plugin_response;
	json_t *request = json_object_get(root, "request");

	const char *request_text = json_string_value(request);

	if(!strcasecmp(request_text, "setup") || !strcasecmp(request_text, "ack") || !strcasecmp(request_text, "restart")) {
		/* These messages are handled asynchronously */
		janus_mutex_unlock(&sessions_mutex);
		janus_frame_message *msg = g_malloc0(sizeof(janus_frame_message));
		msg->handle = handle;
		msg->transaction = transaction;
		msg->message = root;
		msg->jsep = jsep;

		g_async_queue_push(messages, msg);

		return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
	} else {
		JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
		error_code = JANUS_FRAME_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
	}

plugin_response:
		{
			janus_mutex_unlock(&sessions_mutex);
			if(!response) {
				/* Prepare JSON error event */
				response = json_object();
				json_object_set_new(response, "frame", json_string("event"));
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

void janus_frame_setup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "WebRTC media is now available\n");
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_mutex_lock(&sessions_mutex);
	janus_frame_session *session = janus_frame_lookup_session(handle);
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

void janus_frame_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len) {
	/* We don't do audio/video */
}

void janus_frame_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len) {
	/* We don't do audio/video */
}

/* messages received through DataChannel */
void janus_frame_incoming_data(janus_plugin_session *handle, char *buf, int len) {
	if(handle == NULL || handle->stopped || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	/* Incoming request from this user: what should we do? */
	janus_frame_session *session = (janus_frame_session *)handle->plugin_handle;
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
	janus_frame_handle_incoming_request(handle, text, NULL, FALSE);
}

static void* janus_frame_data_handler(void *data)
{
	janus_frame_session *session = janus_frames_data.session;
	int cam = janus_frames_data.cam;
	int hd = janus_frames_data.hd;
	
	janus_mutex_unlock(&data_mutex);
	
	CURLcode res;
	char frameurl[128];
	json_t *jresp, *reply = NULL;
	char *reply_text = NULL;
	string s;
	
	CURL *curl;
	curl = curl_easy_init();
	if(curl == NULL) {
		JANUS_LOG(LOG_ERR, "Can't init CURL\n");
		return NULL;
	}
	
	JANUS_LOG(LOG_VERB, "Frame thread for camera %d (hd=%d) running.\n", cam, hd);
	
	sprintf(frameurl, "%s/frame?id=%llu&cam=%d&hd=%d", SERVICE_URL, session->sdp_sessid, cam, hd);
	curl_easy_setopt(curl, CURLOPT_URL, frameurl);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
	
	while(1)
	{
		init_string(&s);
//JANUS_LOG(LOG_ERR, "Request %d-%d %p %s\n", cam, hd, curl, frameurl);
		res = curl_easy_perform(curl);
//JANUS_LOG(LOG_ERR, "Sending %d-%d\n", cam, hd);
//JANUS_LOG(LOG_ERR, "Sending %d-%d\n%s\n", cam, hd, reply_text);
		int i, k, ready = 0xffffffff;
		/* Invio il frame a tutti gli handle in attesa */
		/* Il lock va anticipato in questo punto per via dell'handle? Provare. */
		janus_mutex_lock(&janus_frames[cam][hd].mutex);
		free(janus_frames[cam][hd].data);
		janus_frames[cam][hd].data = malloc(32+s.len);
		sprintf(janus_frames[cam][hd].data, "{\"frame\":\"success\",\"data\":%s}", s.ptr);
		janus_frames[cam][hd].datalen = strlen(janus_frames[cam][hd].data);
		k = 0;
		if(!strncmp(s.ptr, "{\"key\":1", 8)) k = 1;
		for(i=0; i<32; i++)
		{
			if(janus_frames[cam][hd].request & (1<<i))
			{
//printf("relay cam %d hd %d session %d\n", cam, hd, i);
//fflush(stdout);
//JANUS_LOG(LOG_ERR, "Sending %d-%d %d handle %p\n", cam, hd, i, janus_handle_session[i]);
				//janus_frames[cam][hd].request &= ~(1<<i);
				if((janus_frames[cam][hd].waitkf & (1<<i)) && !k) continue;
				janus_frames[cam][hd].waitkf &= ~(1<<i);
				ready &= ~(1<<i);
				gateway->relay_data(janus_handle_session[i], janus_frames[cam][hd].data, janus_frames[cam][hd].datalen);
			}
		}
		janus_frames[cam][hd].ready = ready;
		janus_mutex_unlock(&janus_frames[cam][hd].mutex);
		
		free(s.ptr);
	}
	
	curl_easy_cleanup(curl);
	
	return NULL;
}

void janus_frame_queue_request(janus_plugin_session *handle, int cam, int hd)
{
	janus_frame_session *session = (janus_frame_session *)handle->plugin_handle;
	//json_t *reply = NULL;
	
	/* Verifico se il frame richiesto sia già disponibile, nel caso lo restituisco immediatamente.
	   Altrimenti faccio partire un thread di richiesta per questa cam+hd (o recupero l'esistente)
	   e prenoto l'invio appena il frame arriva. */
	if((cam < 1) || (cam > 8)) return;
	if(hd) hd = 1;
	
	janus_mutex_lock(&janus_frames[cam][hd].mutex);
#if 0
	if(janus_frames[cam][hd].ready & (1<<session->id))
	{
		gateway->relay_data(handle, janus_frames[cam][hd].data, janus_frames[cam][hd].datalen);
		janus_frames[cam][hd].ready &= ~(1<<session->id);
	}
	else
#endif
	{
		/* Prenoto la richiesta */
		if(!janus_frames[cam][hd].thread)
		{
			GError *error = NULL;
			janus_mutex_lock(&data_mutex);
			janus_frames_data.session = session;
			janus_frames_data.cam = cam;
			janus_frames_data.hd = hd;
			janus_frames[cam][hd].thread = g_thread_try_new("Data handler", janus_frame_data_handler, (void*)&janus_frames_data, &error);
			if(error != NULL) {
				JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the data handler thread...\n", error->code, error->message ? error->message : "??");
				janus_mutex_unlock(&janus_frames[cam][hd].mutex);
				janus_mutex_unlock(&data_mutex);
				return;
			}
		}
		janus_handle_session[session->id] = handle;
		if(!(janus_frames[cam][hd].request & (1<<session->id)))
		{
printf("request cam %d hd %d session %d\n", cam, hd, session->id);
fflush(stdout);
			janus_frames[cam][hd].waitkf |= (1<<session->id);
			janus_frames[cam][hd].request |= (1<<session->id);
		}
	}
	janus_mutex_unlock(&janus_frames[cam][hd].mutex);
}

void janus_frame_stop(janus_plugin_session *handle)
{
	janus_frame_session *session = (janus_frame_session *)handle->plugin_handle;
	int cam;
	for(cam=0; cam<(8+1); cam++)
	{
if(janus_frames[cam][0].request & (1<<session->id)) printf("close cam %d hd %d session %d\n", cam, 0, session->id);
if(janus_frames[cam][1].request & (1<<session->id)) printf("close cam %d hd %d session %d\n", cam, 1, session->id);
		/* MD */
		janus_mutex_lock(&janus_frames[cam][0].mutex);
		janus_frames[cam][0].request &= ~(1<<session->id);
		janus_mutex_unlock(&janus_frames[cam][0].mutex);
		/* HD */
		janus_mutex_lock(&janus_frames[cam][1].mutex);
		janus_frames[cam][1].request &= ~(1<<session->id);
		janus_mutex_unlock(&janus_frames[cam][1].mutex);
	}
	fflush(stdout);
}

janus_plugin_result *janus_frame_handle_incoming_request(janus_plugin_session *handle, char *text, json_t *json, gboolean internal) {
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
	//JANUS_VALIDATE_JSON_OBJECT(root, transaction_parameters,
	//	error_code, error_cause, TRUE,
	//	JANUS_FRAME_ERROR_MISSING_ELEMENT, JANUS_FRAME_ERROR_INVALID_ELEMENT);
	const char *transaction_text = NULL;
	json_t *reply = NULL;
	//if(error_code != 0)
	//	goto msg_response;
	json_t *request = json_object_get(root, "request");
	if(!request) return NULL;
	json_t *transaction = json_object_get(root, "transaction");
	const char *request_text = json_string_value(request);
	transaction_text = json_string_value(transaction);
	
        janus_frame_session *session = (janus_frame_session *)handle->plugin_handle;
        if(!session) return NULL;

	if(!request_text) {
		JANUS_LOG(LOG_ERR, "Empty request\n");
		error_code = JANUS_FRAME_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Empty request");
	} else if(!strcasecmp(request_text, "frame")) {
//JANUS_LOG(LOG_ERR, "** Request frame\n");
		//JANUS_VALIDATE_JSON_OBJECT(root, message_parameters,
		//	error_code, error_cause, TRUE,
		//	JANUS_FRAME_ERROR_MISSING_ELEMENT, JANUS_FRAME_ERROR_INVALID_ELEMENT);
		//if(error_code != 0)
		//	goto msg_response;
		json_t *cam_ref = json_object_get(root, "cam");
		json_t *cam_hd = json_object_get(root, "hd");
		
		if (cam_ref != NULL) {
			int cam_id, hd;
			
			cam_id = json_integer_value(cam_ref);
			hd = 0;
			if(cam_hd != NULL) hd = json_integer_value(cam_hd);
			janus_frame_queue_request(handle, cam_id, hd);
			
			json_decref(root);
			return NULL;
		}
		
		JANUS_LOG(LOG_ERR, "Unsupported request %s\n", request_text);
		error_code = JANUS_FRAME_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unsupported request %s", request_text);
	} else if(!strcasecmp(request_text, "frameinfo")) {
		CURLcode res;
		char frameurl[128];
		char *reply_text = NULL;
		string s;
	
		CURL *curl;
		curl = curl_easy_init();
		if(curl == NULL) {
			JANUS_LOG(LOG_ERR, "Can't init CURL\n");
			return NULL;
		}
		sprintf(frameurl, "%s/frameinfo", SERVICE_URL);
		curl_easy_setopt(curl, CURLOPT_URL, frameurl);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
		
		init_string(&s);
		res = curl_easy_perform(curl);
		
		if(!res)
		{
			reply_text = malloc(64+s.len);
			sprintf(reply_text, "{\"frameinfo\":\"success\",\"data\":%s,\"transaction\":\"%s\"}", s.ptr, transaction_text);
			gateway->relay_data(handle, reply_text, strlen(reply_text));
			free(reply_text);
			free(s.ptr);
			curl_easy_cleanup(curl);
			return NULL;
		}
		else
		{
			free(s.ptr);
			curl_easy_cleanup(curl);
			JANUS_LOG(LOG_ERR, "Unsupported request %s\n", request_text);
			error_code = JANUS_FRAME_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unsupported request %s", request_text);
		}
	} else if(!strcasecmp(request_text, "close")) {
		// TODO: check if session->slot != 0
		JANUS_LOG(LOG_INFO, "Frame: CLOSE session %llu\n", session->sdp_sessid);
		json_object_set_new(reply, "frame", json_string("success"));
		janus_frame_stop(handle);
	} else {
		JANUS_LOG(LOG_ERR, "Unsupported request %s\n", request_text);
		error_code = JANUS_FRAME_ERROR_INVALID_REQUEST;
		g_snprintf(error_cause, 512, "Unsupported request %s", request_text);
	}

msg_response:
		{
			if(!internal) {
				if(error_code == 0 && !reply) {
					error_code = JANUS_FRAME_ERROR_UNKNOWN_ERROR;
					g_snprintf(error_cause, 512, "Invalid response");
				}
				if(error_code != 0) {
					/* Prepare JSON error event */
					json_t *event = json_object();
					json_object_set_new(event, "frame", json_string("error"));
					json_object_set_new(event, "error_code", json_integer(error_code));
					json_object_set_new(event, "error", json_string(error_cause));
					reply = event;
				}
				//if(transaction_text && json == NULL)
				//	json_object_set_new(reply, "transaction", json_string(transaction_text));
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

void janus_frame_slow_link(janus_plugin_session *handle, int uplink, int video) {
	/* We don't do audio/video */
}

void janus_frame_hangup_media(janus_plugin_session *handle) {
	janus_mutex_lock(&sessions_mutex);
	janus_frame_stop(handle);
	janus_frame_hangup_media_internal(handle);
	janus_mutex_unlock(&sessions_mutex);
}

static void janus_frame_hangup_media_internal(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "No WebRTC media anymore\n");
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_frame_session *session = janus_frame_lookup_session(handle);
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
static void *janus_frame_handler(void *data) {
	JANUS_LOG(LOG_VERB, "Joining frame handler thread\n");
	janus_frame_message *msg = NULL;
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
			janus_frame_message_free(msg);
			continue;
		}
		janus_mutex_lock(&sessions_mutex);
		janus_frame_session *session = janus_frame_lookup_session(msg->handle);
		if(!session) {
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			janus_frame_message_free(msg);
			continue;
		}
		if(session->destroyed) {
			janus_mutex_unlock(&sessions_mutex);
			janus_frame_message_free(msg);
			continue;
		}
		janus_mutex_unlock(&sessions_mutex);
		/* Handle request */
		error_code = 0;
		root = msg->message;
		if(msg->message == NULL) {
			JANUS_LOG(LOG_ERR, "No message??\n");
			error_code = JANUS_FRAME_ERROR_NO_MESSAGE;
			g_snprintf(error_cause, 512, "%s", "No message??");
			goto error;
		}
		if(!json_is_object(root)) {
			JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
			error_code = JANUS_FRAME_ERROR_INVALID_JSON;
			g_snprintf(error_cause, 512, "JSON error: not an object");
			goto error;
		}
		/* Parse request */
		JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
			error_code, error_cause, TRUE,
			JANUS_FRAME_ERROR_MISSING_ELEMENT, JANUS_FRAME_ERROR_INVALID_ELEMENT);
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
				error_code = JANUS_FRAME_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection already setup");
				goto error;
			}
			do_offer = TRUE;
		} else if(!strcasecmp(request_text, "restart")) {
			if(!g_atomic_int_get(&session->setup)) {
				JANUS_LOG(LOG_ERR, "PeerConnection not setup\n");
				error_code = JANUS_FRAME_ERROR_ALREADY_SETUP;
				g_snprintf(error_cause, 512, "PeerConnection not setup");
				goto error;
			}
			sdp_update = TRUE;
			do_offer = TRUE;
		} else if(!strcasecmp(request_text, "ack")) {
			/* The peer sent their answer back: do nothing */
		} else {
			JANUS_LOG(LOG_VERB, "Unknown request '%s'\n", request_text);
			error_code = JANUS_FRAME_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
			goto error;
		}

		/* Prepare JSON event */
		json_t *event = json_object();
		json_object_set_new(event, "frame", json_string("event"));
		json_object_set_new(event, "result", json_string("ok"));
		if(!do_offer) {
			int ret = gateway->push_event(msg->handle, &janus_frame_plugin, msg->transaction, event, NULL);
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
			int res = gateway->push_event(msg->handle, &janus_frame_plugin, msg->transaction, event, jsep);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (took %"SCNu64" us)\n",
				res, janus_get_monotonic_time()-start);
			json_decref(jsep);
		}
		json_decref(event);
		janus_frame_message_free(msg);
		continue;

error:
		{
			/* Prepare JSON error event */
			json_t *event = json_object();
			json_object_set_new(event, "frame", json_string("error"));
			json_object_set_new(event, "error_code", json_integer(error_code));
			json_object_set_new(event, "error", json_string(error_cause));
			int ret = gateway->push_event(msg->handle, &janus_frame_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
			json_decref(event);
			janus_frame_message_free(msg);
		}
	}
	JANUS_LOG(LOG_VERB, "Leaving frame handler thread\n");
	return NULL;
}
