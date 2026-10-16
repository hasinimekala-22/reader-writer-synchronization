#include <gtk/gtk.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define READER_COUNT 4
#define WRITER_COUNT 2
#define THREAD_COUNT (READER_COUNT + WRITER_COUNT)
#define INITIAL_VALUE 100

/* Synchronization primitives */
static pthread_mutex_t reader_count_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t model_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t control_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t control_cond = PTHREAD_COND_INITIALIZER;
static sem_t resource_sem;

static int reader_count = 0;
static int shared_value = INITIAL_VALUE;
static int completed_writes = 0;
static int completed_threads = 0;

static char *thread_states[THREAD_COUNT];
static char *thread_details[THREAD_COUNT];

static const char *thread_names[THREAD_COUNT] = {
    "Reader 1", "Reader 2", "Reader 3", "Reader 4",
    "Writer 1", "Writer 2"
};

static pthread_t worker_threads[THREAD_COUNT];
static bool thread_created[THREAD_COUNT];
static bool threads_need_join = false;

typedef struct {
    int id;
} WorkerArg;

static WorkerArg worker_args[THREAD_COUNT];

static bool running = false;
static bool paused = false;
static bool step_mode = false;
static unsigned int step_budget = 0;
static double simulation_speed = 1.0;

/* GTK widgets */
static GtkWidget *resource_value_label;
static GtkWidget *metric_shared_value_label;
static GtkWidget *resource_status_label;
static GtkWidget *active_readers_label;
static GtkWidget *writes_label;
static GtkWidget *finished_label;
static GtkWidget *status_label;
static GtkWidget *thread_state_labels[THREAD_COUNT];
static GtkWidget *thread_detail_labels[THREAD_COUNT];
static GtkWidget *thread_cards[THREAD_COUNT];
static GtkWidget *start_button;
static GtkWidget *pause_button;
static GtkWidget *resume_button;
static GtkWidget *step_button;
static GtkWidget *reset_button;
static GtkWidget *step_check;
static GtkWidget *speed_scale;
static GtkWidget *progress_bar;
static GtkTextBuffer *log_buffer;

/* ---------- Logging ---------- */

static void append_log(const char *message)
{
    GtkTextIter end;
    GDateTime *now = g_date_time_new_now_local();
    char *stamp = g_date_time_format(now, "%H:%M:%S");
    char *line = g_strdup_printf("%s   %s\n", stamp, message);

    gtk_text_buffer_get_end_iter(log_buffer, &end);
    gtk_text_buffer_insert(log_buffer, &end, line, -1);

    g_free(line);
    g_free(stamp);
    g_date_time_unref(now);
}

typedef struct {
    char *message;
} LogEvent;

static gboolean append_log_idle(gpointer data)
{
    LogEvent *event = data;

    append_log(event->message);

    g_free(event->message);
    g_free(event);

    return G_SOURCE_REMOVE;
}

static void log_event(const char *message)
{
    LogEvent *event = g_new0(LogEvent, 1);
    event->message = g_strdup(message);

    /* GTK widgets are updated only on the GTK main thread. */
    g_idle_add(append_log_idle, event);
}

/* ---------- Thread state ---------- */

static const char *state_class(const char *state)
{
    if (strcmp(state, "READING") == 0)
        return "state-reading";

    if (strcmp(state, "WRITING") == 0)
        return "state-writing";

    if (strcmp(state, "WAITING") == 0)
        return "state-waiting";

    if (strcmp(state, "DONE") == 0)
        return "state-done";

    return "state-idle";
}

static gboolean refresh_ui_idle(gpointer unused);

static void request_refresh(void)
{
    g_idle_add(refresh_ui_idle, NULL);
}

static void set_thread_state(int id,
                             const char *state,
                             const char *detail)
{
    pthread_mutex_lock(&model_mutex);

    g_free(thread_states[id]);
    g_free(thread_details[id]);

    thread_states[id] = g_strdup(state);
    thread_details[id] = g_strdup(detail);

    pthread_mutex_unlock(&model_mutex);

    request_refresh();
}

/* ---------- Speed and step controls ---------- */

static void sleep_at_speed(unsigned int base_ms)
{
    pthread_mutex_lock(&control_mutex);
    double speed = simulation_speed;
    pthread_mutex_unlock(&control_mutex);

    unsigned int delay = (unsigned int)(base_ms / speed);

    if (delay < 80)
        delay = 80;

    struct timespec ts = {
        .tv_sec = delay / 1000,
        .tv_nsec = (long)(delay % 1000) * 1000000L
    };

    nanosleep(&ts, NULL);
}

static bool simulation_checkpoint(void)
{
    pthread_mutex_lock(&control_mutex);

    while (running &&
           (paused || (step_mode && step_budget == 0))) {
        pthread_cond_wait(&control_cond, &control_mutex);
    }

    bool keep_going = running;

    if (keep_going && step_mode && step_budget > 0)
        step_budget--;

    pthread_mutex_unlock(&control_mutex);

    return keep_going;
}

/* ---------- Completion handling ---------- */

static void worker_finished(void)
{
    pthread_mutex_lock(&model_mutex);

    completed_threads++;
    bool all_done = completed_threads == THREAD_COUNT;

    pthread_mutex_unlock(&model_mutex);

    if (all_done) {
        pthread_mutex_lock(&control_mutex);

        running = false;
        pthread_cond_broadcast(&control_cond);

        pthread_mutex_unlock(&control_mutex);

        log_event("All worker threads have completed.");
    }

    request_refresh();
}

/* ---------- Reader thread ---------- */

static void *reader_worker(void *ptr)
{
    WorkerArg *arg = ptr;
    int id = arg->id;
    char message[180];

    sleep_at_speed((unsigned int)(150 + id * 100));

    if (!simulation_checkpoint())
        goto finished;

    set_thread_state(id, "WAITING", "Requesting read access");

    snprintf(message, sizeof(message),
             "%s requested read access.", thread_names[id]);
    log_event(message);

    /*
     * First reader locks the resource.
     * Additional readers may enter together.
     */
    pthread_mutex_lock(&reader_count_mutex);

    reader_count++;

    if (reader_count == 1) {
        if (sem_wait(&resource_sem) != 0) {
            reader_count--;
            pthread_mutex_unlock(&reader_count_mutex);
            goto finished;
        }
    }

    pthread_mutex_unlock(&reader_count_mutex);

    if (!simulation_checkpoint()) {
        pthread_mutex_lock(&reader_count_mutex);

        reader_count--;

        if (reader_count == 0)
            sem_post(&resource_sem);

        pthread_mutex_unlock(&reader_count_mutex);

        goto finished;
    }

    set_thread_state(id, "READING", "Reading shared value");

    pthread_mutex_lock(&model_mutex);
    int observed = shared_value;
    pthread_mutex_unlock(&model_mutex);

    snprintf(message, sizeof(message),
             "%s is reading the shared value: %d.",
             thread_names[id], observed);
    log_event(message);

    sleep_at_speed(900);

    /* Last reader releases the shared resource. */
    pthread_mutex_lock(&reader_count_mutex);

    reader_count--;

    bool last_reader = reader_count == 0;

    if (last_reader)
        sem_post(&resource_sem);

    pthread_mutex_unlock(&reader_count_mutex);

    set_thread_state(id, "DONE", "Read completed");

    snprintf(message, sizeof(message),
             "%s finished reading%s.",
             thread_names[id],
             last_reader ? "; resource released" : "");

    log_event(message);

finished:
    worker_finished();
    return NULL;
}

/* ---------- Writer thread ---------- */

static void *writer_worker(void *ptr)
{
    WorkerArg *arg = ptr;
    int id = arg->id;
    char message[180];

    sleep_at_speed((unsigned int)(500 + (id - READER_COUNT) * 250));

    if (!simulation_checkpoint())
        goto finished;

    set_thread_state(id, "WAITING",
                     "Waiting for exclusive access");

    snprintf(message, sizeof(message),
             "%s requested exclusive write access.",
             thread_names[id]);
    log_event(message);

    /* Only one writer, or one reader group, can hold the resource. */
    if (sem_wait(&resource_sem) != 0)
        goto finished;

    if (!simulation_checkpoint()) {
        sem_post(&resource_sem);
        goto finished;
    }

    set_thread_state(id, "WRITING", "Exclusive access granted");

    snprintf(message, sizeof(message),
             "%s acquired the resource lock.", thread_names[id]);
    log_event(message);

    sleep_at_speed(650);

    pthread_mutex_lock(&model_mutex);

    int before = shared_value;
    shared_value += 10;
    int after = shared_value;
    completed_writes++;

    pthread_mutex_unlock(&model_mutex);

    snprintf(message, sizeof(message),
             "%s updated the shared value: %d -> %d.",
             thread_names[id], before, after);

    log_event(message);
    request_refresh();

    sem_post(&resource_sem);

    set_thread_state(id, "DONE",
                     "Write completed; lock released");

    snprintf(message, sizeof(message),
             "%s released the resource lock.", thread_names[id]);
    log_event(message);

finished:
    worker_finished();
    return NULL;
}

/* ---------- UI refresh ---------- */

static gboolean refresh_ui_idle(gpointer unused)
{
    (void)unused;

    int value, writes, finished;
    int readers = 0, writers = 0;

    char *states[THREAD_COUNT];
    char *details[THREAD_COUNT];

    pthread_mutex_lock(&model_mutex);

    value = shared_value;
    writes = completed_writes;
    finished = completed_threads;

    for (int i = 0; i < THREAD_COUNT; i++) {
        states[i] = g_strdup(
            thread_states[i] ? thread_states[i] : "IDLE");

        details[i] = g_strdup(
            thread_details[i] ? thread_details[i] : "Ready to run");

        if (strcmp(states[i], "READING") == 0)
            readers++;

        if (strcmp(states[i], "WRITING") == 0)
            writers++;
    }

    pthread_mutex_unlock(&model_mutex);

    char *number = g_strdup_printf("%d", value);
    char *reader_text = g_strdup_printf("%d / %d",
                                        readers, READER_COUNT);
    char *write_text = g_strdup_printf("%d / %d",
                                       writes, WRITER_COUNT);
    char *finished_text = g_strdup_printf("%d / %d",
                                          finished, THREAD_COUNT);

    gtk_label_set_text(GTK_LABEL(resource_value_label), number);
    gtk_label_set_text(GTK_LABEL(metric_shared_value_label), number);
    gtk_label_set_text(GTK_LABEL(active_readers_label), reader_text);
    gtk_label_set_text(GTK_LABEL(writes_label), write_text);
    gtk_label_set_text(GTK_LABEL(finished_label), finished_text);

    if (writers)
        gtk_label_set_text(GTK_LABEL(resource_status_label),
                           "EXCLUSIVE WRITE IN PROGRESS");
    else if (readers)
        gtk_label_set_text(GTK_LABEL(resource_status_label),
                           "READ ACCESS ACTIVE");
    else
        gtk_label_set_text(GTK_LABEL(resource_status_label),
                           "RESOURCE AVAILABLE");

    gtk_progress_bar_set_fraction(
        GTK_PROGRESS_BAR(progress_bar),
        (double)finished / THREAD_COUNT);

    pthread_mutex_lock(&control_mutex);

    bool is_running = running;
    bool is_paused = paused;

    pthread_mutex_unlock(&control_mutex);

    if (is_running && is_paused) {
        gtk_label_set_text(GTK_LABEL(status_label),
            "PAUSED - workers wait at checkpoints");
    } else if (is_running) {
        gtk_label_set_text(GTK_LABEL(status_label),
            "RUNNING - live pthread synchronization");
    } else if (finished == THREAD_COUNT &&
               value == 120 && writes == 2) {
        gtk_label_set_text(GTK_LABEL(status_label),
            "PASSED - final shared value is 120");
    } else if (finished == THREAD_COUNT) {
        gtk_label_set_text(GTK_LABEL(status_label),
            "COMPLETED - review the execution log");
    } else {
        gtk_label_set_text(GTK_LABEL(status_label),
            "READY - press Start to begin");
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
        gtk_label_set_text(
            GTK_LABEL(thread_state_labels[i]), states[i]);

        gtk_label_set_text(
            GTK_LABEL(thread_detail_labels[i]), details[i]);

        const char *css = state_class(states[i]);

        gtk_widget_remove_css_class(thread_cards[i], "state-reading");
        gtk_widget_remove_css_class(thread_cards[i], "state-writing");
        gtk_widget_remove_css_class(thread_cards[i], "state-waiting");
        gtk_widget_remove_css_class(thread_cards[i], "state-done");
        gtk_widget_remove_css_class(thread_cards[i], "state-idle");

        gtk_widget_add_css_class(thread_cards[i], css);

        g_free(states[i]);
        g_free(details[i]);
    }

    gtk_widget_set_sensitive(start_button, !is_running);
    gtk_widget_set_sensitive(pause_button, is_running && !is_paused);
    gtk_widget_set_sensitive(resume_button, is_running && is_paused);
    gtk_widget_set_sensitive(step_button, is_running);
    gtk_widget_set_sensitive(reset_button, !is_running);

    g_free(number);
    g_free(reader_text);
    g_free(write_text);
    g_free(finished_text);

    if (!is_running && finished == THREAD_COUNT && threads_need_join) {
        for (int i = 0; i < THREAD_COUNT; i++) {
            if (thread_created[i]) {
                pthread_join(worker_threads[i], NULL);
                thread_created[i] = false;
            }
        }

        threads_need_join = false;
    }

    return G_SOURCE_REMOVE;
}

/* ---------- Button callbacks ---------- */

static void on_start(GtkButton *button, gpointer data)
{
    (void)button;
    (void)data;

    pthread_mutex_lock(&control_mutex);

    if (running) {
        pthread_mutex_unlock(&control_mutex);
        return;
    }

    running = true;
    paused = false;
    step_budget = 0;

    pthread_mutex_unlock(&control_mutex);

    pthread_mutex_lock(&model_mutex);

    shared_value = INITIAL_VALUE;
    completed_writes = 0;
    completed_threads = 0;
    reader_count = 0;

    for (int i = 0; i < THREAD_COUNT; i++) {
        g_free(thread_states[i]);
        g_free(thread_details[i]);

        thread_states[i] = g_strdup("IDLE");
        thread_details[i] = g_strdup("Ready to run");
    }

    pthread_mutex_unlock(&model_mutex);

    gtk_text_buffer_set_text(log_buffer, "", -1);

    append_log("Simulation started. Shared value = 100.");
    append_log("Readers may share access; writers require exclusive access.");
    append_log("Expected result after two writes: 120.");

    for (int i = 0; i < THREAD_COUNT; i++) {
        worker_args[i].id = i;
        thread_created[i] = false;
    }

    threads_need_join = true;

    for (int i = 0; i < THREAD_COUNT; i++) {
        void *(*worker)(void *) =
            (i < READER_COUNT) ? reader_worker : writer_worker;

        int rc = pthread_create(
            &worker_threads[i], NULL, worker, &worker_args[i]);

        if (rc == 0) {
            thread_created[i] = true;
        } else {
            char msg[120];

            snprintf(msg, sizeof(msg),
                     "Could not create %s (error %d).",
                     thread_names[i], rc);

            log_event(msg);
            worker_finished();
        }
    }

    request_refresh();
}

static void on_pause(GtkButton *button, gpointer data)
{
    (void)button;
    (void)data;

    pthread_mutex_lock(&control_mutex);
    paused = true;
    pthread_mutex_unlock(&control_mutex);

    log_event("Pause requested; workers stop at their next checkpoint.");
    request_refresh();
}

static void on_resume(GtkButton *button, gpointer data)
{
    (void)button;
    (void)data;

    pthread_mutex_lock(&control_mutex);

    paused = false;
    pthread_cond_broadcast(&control_cond);

    pthread_mutex_unlock(&control_mutex);

    log_event("Simulation resumed.");
    request_refresh();
}

static void on_step(GtkButton *button, gpointer data)
{
    (void)button;
    (void)data;

    pthread_mutex_lock(&control_mutex);

    if (running) {
        paused = false;
        step_budget++;
        pthread_cond_broadcast(&control_cond);
    }

    pthread_mutex_unlock(&control_mutex);

    log_event("Single-step token released.");
    request_refresh();
}

static void on_step_mode(GtkCheckButton *button, gpointer data)
{
    (void)data;

    pthread_mutex_lock(&control_mutex);

    step_mode = gtk_check_button_get_active(button);

    if (!step_mode) {
        step_budget = 0;
        pthread_cond_broadcast(&control_cond);
    }

    bool enabled = step_mode;

    pthread_mutex_unlock(&control_mutex);

    log_event(enabled ? "Step mode enabled." : "Step mode disabled.");
}

static void on_speed_changed(GtkRange *range, gpointer data)
{
    (void)data;

    pthread_mutex_lock(&control_mutex);

    simulation_speed = gtk_range_get_value(range);
    double speed = simulation_speed;

    pthread_mutex_unlock(&control_mutex);

    char msg[100];

    snprintf(msg, sizeof(msg), "Simulation speed: %.1fx.", speed);

    gtk_label_set_text(GTK_LABEL(status_label), msg);
}

static void on_reset(GtkButton *button, gpointer data)
{
    (void)button;
    (void)data;

    pthread_mutex_lock(&control_mutex);

    if (running) {
        pthread_mutex_unlock(&control_mutex);
        return;
    }

    paused = false;
    step_budget = 0;

    pthread_mutex_unlock(&control_mutex);

    pthread_mutex_lock(&model_mutex);

    shared_value = INITIAL_VALUE;
    completed_writes = 0;
    completed_threads = 0;
    reader_count = 0;

    for (int i = 0; i < THREAD_COUNT; i++) {
        g_free(thread_states[i]);
        g_free(thread_details[i]);

        thread_states[i] = g_strdup("IDLE");
        thread_details[i] = g_strdup("Ready to run");
    }

    pthread_mutex_unlock(&model_mutex);

    gtk_text_buffer_set_text(log_buffer, "", -1);
    append_log("Simulation reset. Ready for a new run.");

    request_refresh();
}

/* ---------- UI construction ---------- */

static GtkWidget *make_metric(const char *heading,
                              GtkWidget **value_out,
                              const char *initial,
                              const char *accent_class)
{
    GtkWidget *card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 7);
    gtk_widget_add_css_class(card, "metric-card");

    GtkWidget *heading_label = gtk_label_new(heading);
    gtk_widget_add_css_class(heading_label, "eyebrow");
    gtk_label_set_xalign(GTK_LABEL(heading_label), 0.0f);

    GtkWidget *value = gtk_label_new(initial);
    gtk_widget_add_css_class(value, "metric-value");

    if (accent_class)
        gtk_widget_add_css_class(value, accent_class);

    gtk_label_set_xalign(GTK_LABEL(value), 0.0f);

    gtk_box_append(GTK_BOX(card), heading_label);
    gtk_box_append(GTK_BOX(card), value);

    *value_out = value;

    return card;
}

static GtkWidget *make_thread_card(int id)
{
    GtkWidget *card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);

    gtk_widget_add_css_class(card, "thread-card");
    gtk_widget_add_css_class(card, "state-idle");

    GtkWidget *top = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);

    GtkWidget *name = gtk_label_new(thread_names[id]);
    gtk_widget_add_css_class(name, "thread-name");
    gtk_label_set_xalign(GTK_LABEL(name), 0.0f);
    gtk_widget_set_hexpand(name, TRUE);

    GtkWidget *state = gtk_label_new("IDLE");
    gtk_widget_add_css_class(state, "state-pill");

    gtk_box_append(GTK_BOX(top), name);
    gtk_box_append(GTK_BOX(top), state);

    GtkWidget *detail = gtk_label_new("Ready to run");
    gtk_label_set_xalign(GTK_LABEL(detail), 0.0f);
    gtk_label_set_wrap(GTK_LABEL(detail), TRUE);
    gtk_widget_add_css_class(detail, "muted");

    gtk_box_append(GTK_BOX(card), top);
    gtk_box_append(GTK_BOX(card), detail);

    thread_state_labels[id] = state;
    thread_detail_labels[id] = detail;
    thread_cards[id] = card;

    return card;
}

static void add_section_heading(GtkBox *parent,
                                const char *title,
                                const char *subtitle)
{
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 4);

    GtkWidget *heading = gtk_label_new(title);
    gtk_widget_add_css_class(heading, "section-heading");
    gtk_label_set_xalign(GTK_LABEL(heading), 0.0f);

    GtkWidget *sub = gtk_label_new(subtitle);
    gtk_widget_add_css_class(sub, "muted");
    gtk_label_set_xalign(GTK_LABEL(sub), 0.0f);
    gtk_label_set_wrap(GTK_LABEL(sub), TRUE);

    gtk_box_append(GTK_BOX(box), heading);
    gtk_box_append(GTK_BOX(box), sub);
    gtk_box_append(parent, box);
}

/* ---------- Minimal academic styling ---------- */

static void load_css(void)
{
    const char *css =
        "window { background: #f4f6f9; color: #172033; }"
        ".page { padding: 24px; }"
        ".hero { background: #ffffff; border: 1px solid #e1e6ee;"
        " border-radius: 16px; padding: 22px; }"
        ".app-title { font-size: 25px; font-weight: 800; color: #172033; }"
        ".app-subtitle, .muted { color: #667085; font-size: 12px; }"
        ".eyebrow { color: #667085; font-size: 10px;"
        " font-weight: 800; letter-spacing: 1px; }"
        ".metric-card { background: #ffffff; border: 1px solid #e1e6ee;"
        " border-radius: 13px; padding: 16px; }"
        ".metric-value { color: #172033; font-size: 25px; font-weight: 800; }"
        ".accent-blue { color: #1769aa; }"
        ".accent-green { color: #16805d; }"
        ".accent-purple { color: #6654a8; }"
        ".section-heading { color: #172033; font-size: 16px; font-weight: 800; }"
        ".panel { background: #ffffff; border: 1px solid #e1e6ee;"
        " border-radius: 14px; padding: 16px; }"
        ".thread-card { background: #ffffff; border: 1px solid #e1e6ee;"
        " border-left: 4px solid #c6ced9; border-radius: 10px; padding: 13px; }"
        ".thread-name { color: #172033; font-weight: 700; font-size: 13px; }"
        ".state-pill { background: #edf0f4; color: #667085;"
        " border-radius: 6px; padding: 4px 7px; font-size: 10px; font-weight: 800; }"
        ".state-reading { border-left-color: #16805d; }"
        ".state-reading .state-pill { background: #e5f6ef; color: #16805d; }"
        ".state-writing { border-left-color: #c27a19; }"
        ".state-writing .state-pill { background: #fff1d9; color: #945b0b; }"
        ".state-waiting { border-left-color: #5578c9; }"
        ".state-waiting .state-pill { background: #eaf0ff; color: #3156a6; }"
        ".state-done { border-left-color: #16805d; }"
        ".state-done .state-pill { background: #e5f6ef; color: #16805d; }"
        ".state-idle { border-left-color: #c6ced9; }"
        ".resource-number { font-size: 42px; font-weight: 850; color: #1769aa; }"
        ".status-strip { background: #edf4fb; border: 1px solid #d8e6f5;"
        " border-radius: 10px; padding: 11px 13px; color: #24557e;"
        " font-weight: 700; font-size: 12px; }"
        ".log-view { background: #fbfcfe; color: #344054;"
        " font-family: monospace; font-size: 11px; padding: 12px; }"
        "button { border-radius: 8px; padding: 8px 13px; font-weight: 700; }"
        "button.suggested-action { background: #1769aa; color: white; }"
        "textview { background: #fbfcfe; }";

    GtkCssProvider *provider = gtk_css_provider_new();

    gtk_css_provider_load_from_string(provider, css);

    gtk_style_context_add_provider_for_display(
        gdk_display_get_default(),
        GTK_STYLE_PROVIDER(provider),
        GTK_STYLE_PROVIDER_PRIORITY_APPLICATION);

    g_object_unref(provider);
}

/* ---------- Main application window ---------- */

static void activate(GtkApplication *app, gpointer data)
{
    (void)data;

    load_css();

    GtkWidget *window = gtk_application_window_new(app);

    gtk_window_set_title(
        GTK_WINDOW(window),
        "SYNC LAB | Reader-Writer Synchronization");

    gtk_window_set_default_size(GTK_WINDOW(window), 1100, 850);

    GtkWidget *scroll = gtk_scrolled_window_new();

    gtk_scrolled_window_set_policy(
        GTK_SCROLLED_WINDOW(scroll),
        GTK_POLICY_NEVER,
        GTK_POLICY_AUTOMATIC);

    gtk_window_set_child(GTK_WINDOW(window), scroll);

    GtkWidget *page = gtk_box_new(GTK_ORIENTATION_VERTICAL, 18);
    gtk_widget_add_css_class(page, "page");

    gtk_scrolled_window_set_child(
        GTK_SCROLLED_WINDOW(scroll), page);

    /* Header */
    GtkWidget *hero = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 20);
    gtk_widget_add_css_class(hero, "hero");

    GtkWidget *hero_text = gtk_box_new(GTK_ORIENTATION_VERTICAL, 5);
    gtk_widget_set_hexpand(hero_text, TRUE);

    GtkWidget *title = gtk_label_new("SYNC LAB");
    gtk_widget_add_css_class(title, "app-title");
    gtk_label_set_xalign(GTK_LABEL(title), 0.0f);

    GtkWidget *subtitle = gtk_label_new(
        "Reader-Writer Synchronization Simulator | Operating Systems Lab");

    gtk_widget_add_css_class(subtitle, "app-subtitle");
    gtk_label_set_xalign(GTK_LABEL(subtitle), 0.0f);
    gtk_label_set_wrap(GTK_LABEL(subtitle), TRUE);

    gtk_box_append(GTK_BOX(hero_text), title);
    gtk_box_append(GTK_BOX(hero_text), subtitle);

    GtkWidget *tag = gtk_label_new("C  /  GTK4  /  PTHREADS");
    gtk_widget_add_css_class(tag, "eyebrow");
    gtk_widget_set_valign(tag, GTK_ALIGN_CENTER);

    gtk_box_append(GTK_BOX(hero), hero_text);
    gtk_box_append(GTK_BOX(hero), tag);
    gtk_box_append(GTK_BOX(page), hero);

    /* Metric cards */
    GtkWidget *metrics = gtk_grid_new();

    gtk_grid_set_column_spacing(GTK_GRID(metrics), 12);
    gtk_grid_set_row_spacing(GTK_GRID(metrics), 12);
    gtk_widget_set_hexpand(metrics, TRUE);

    GtkWidget *m1 = make_metric(
        "SHARED RESOURCE",
        &metric_shared_value_label, "100", "accent-blue");

    GtkWidget *m2 = make_metric(
        "ACTIVE READERS",
        &active_readers_label, "0 / 4", "accent-green");

    GtkWidget *m3 = make_metric(
        "COMPLETED WRITES",
        &writes_label, "0 / 2", "accent-purple");

    GtkWidget *m4 = make_metric(
        "THREADS FINISHED",
        &finished_label, "0 / 6", NULL);

    gtk_grid_attach(GTK_GRID(metrics), m1, 0, 0, 1, 1);
    gtk_grid_attach(GTK_GRID(metrics), m2, 1, 0, 1, 1);
    gtk_grid_attach(GTK_GRID(metrics), m3, 2, 0, 1, 1);
    gtk_grid_attach(GTK_GRID(metrics), m4, 3, 0, 1, 1);

    gtk_box_append(GTK_BOX(page), metrics);

    /* Shared resource panel */
    GtkWidget *resource_panel =
        gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);

    gtk_widget_add_css_class(resource_panel, "panel");

    add_section_heading(
        GTK_BOX(resource_panel),
        "Shared resource",
        "Readers can access the value together. A writer must hold "
        "the resource semaphore exclusively.");

    GtkWidget *resource_row =
        gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 16);

    GtkWidget *number_box =
        gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);

    resource_value_label = gtk_label_new("100");
    gtk_widget_add_css_class(resource_value_label, "resource-number");
    gtk_label_set_xalign(GTK_LABEL(resource_value_label), 0.0f);

    gtk_box_append(GTK_BOX(number_box), resource_value_label);

    GtkWidget *value_caption = gtk_label_new("Current value");
    gtk_widget_add_css_class(value_caption, "muted");
    gtk_label_set_xalign(GTK_LABEL(value_caption), 0.0f);

    gtk_box_append(GTK_BOX(number_box), value_caption);

    GtkWidget *resource_info =
        gtk_box_new(GTK_ORIENTATION_VERTICAL, 7);

    gtk_widget_set_hexpand(resource_info, TRUE);

    resource_status_label = gtk_label_new("RESOURCE AVAILABLE");
    gtk_widget_add_css_class(resource_status_label, "eyebrow");
    gtk_label_set_xalign(GTK_LABEL(resource_status_label), 0.0f);

    GtkWidget *equation = gtk_label_new("100  ->  110  ->  120");
    gtk_widget_add_css_class(equation, "section-heading");
    gtk_label_set_xalign(GTK_LABEL(equation), 0.0f);

    GtkWidget *eq_caption = gtk_label_new(
        "Each writer adds 10. Expected final value: 120.");

    gtk_widget_add_css_class(eq_caption, "muted");
    gtk_label_set_xalign(GTK_LABEL(eq_caption), 0.0f);

    gtk_box_append(GTK_BOX(resource_info), resource_status_label);
    gtk_box_append(GTK_BOX(resource_info), equation);
    gtk_box_append(GTK_BOX(resource_info), eq_caption);

    gtk_box_append(GTK_BOX(resource_row), number_box);
    gtk_box_append(GTK_BOX(resource_row), resource_info);
    gtk_box_append(GTK_BOX(resource_panel), resource_row);
    gtk_box_append(GTK_BOX(page), resource_panel);

    /* Thread activity */
    add_section_heading(
        GTK_BOX(page),
        "Thread activity",
        "Each card reflects the state of a real worker thread.");

    GtkWidget *thread_grid = gtk_grid_new();

    gtk_grid_set_column_spacing(GTK_GRID(thread_grid), 12);
    gtk_grid_set_row_spacing(GTK_GRID(thread_grid), 12);

    for (int i = 0; i < THREAD_COUNT; i++) {
        GtkWidget *card = make_thread_card(i);

        gtk_grid_attach(
            GTK_GRID(thread_grid), card, i % 3, i / 3, 1, 1);
    }

    gtk_box_append(GTK_BOX(page), thread_grid);

    /* Controls */
    GtkWidget *control_panel =
        gtk_box_new(GTK_ORIENTATION_VERTICAL, 12);

    gtk_widget_add_css_class(control_panel, "panel");

    add_section_heading(
        GTK_BOX(control_panel),
        "Simulation controls",
        "Pause takes effect at a worker checkpoint. Step mode "
        "releases one checkpoint at a time.");

    GtkWidget *buttons =
        gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);

    start_button = gtk_button_new_with_label("Start");
    gtk_widget_add_css_class(start_button, "suggested-action");

    pause_button = gtk_button_new_with_label("Pause");
    resume_button = gtk_button_new_with_label("Resume");
    step_button = gtk_button_new_with_label("Step");
    reset_button = gtk_button_new_with_label("Reset");

    gtk_box_append(GTK_BOX(buttons), start_button);
    gtk_box_append(GTK_BOX(buttons), pause_button);
    gtk_box_append(GTK_BOX(buttons), resume_button);
    gtk_box_append(GTK_BOX(buttons), step_button);
    gtk_box_append(GTK_BOX(buttons), reset_button);

    gtk_box_append(GTK_BOX(control_panel), buttons);

    GtkWidget *settings =
        gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 18);

    step_check = gtk_check_button_new_with_label("Enable step mode");
    gtk_box_append(GTK_BOX(settings), step_check);

    GtkWidget *speed_box =
        gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);

    GtkWidget *speed_label = gtk_label_new("Speed");
    gtk_widget_add_css_class(speed_label, "muted");

    speed_scale = gtk_scale_new_with_range(
        GTK_ORIENTATION_HORIZONTAL, 0.5, 2.0, 0.5);

    gtk_range_set_value(GTK_RANGE(speed_scale), 1.0);
    gtk_scale_set_draw_value(GTK_SCALE(speed_scale), TRUE);
    gtk_widget_set_size_request(speed_scale, 190, -1);

    gtk_box_append(GTK_BOX(speed_box), speed_label);
    gtk_box_append(GTK_BOX(speed_box), speed_scale);

    gtk_widget_set_hexpand(speed_box, TRUE);

    gtk_box_append(GTK_BOX(settings), speed_box);
    gtk_box_append(GTK_BOX(control_panel), settings);
    gtk_box_append(GTK_BOX(page), control_panel);

    /* Status and progress */
    status_label = gtk_label_new("READY - press Start to begin");
    gtk_widget_add_css_class(status_label, "status-strip");
    gtk_label_set_xalign(GTK_LABEL(status_label), 0.0f);

    gtk_box_append(GTK_BOX(page), status_label);

    progress_bar = gtk_progress_bar_new();
    gtk_progress_bar_set_show_text(GTK_PROGRESS_BAR(progress_bar), TRUE);
    gtk_progress_bar_set_text(
        GTK_PROGRESS_BAR(progress_bar), "Workers completed");

    gtk_box_append(GTK_BOX(page), progress_bar);

    /* Execution log */
    GtkWidget *log_panel =
        gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);

    gtk_widget_add_css_class(log_panel, "panel");

    add_section_heading(
        GTK_BOX(log_panel),
        "Execution log",
        "Events are recorded as worker threads request, acquire, "
        "and release access.");

    GtkWidget *log_scroll = gtk_scrolled_window_new();

    gtk_scrolled_window_set_policy(
        GTK_SCROLLED_WINDOW(log_scroll),
        GTK_POLICY_AUTOMATIC,
        GTK_POLICY_AUTOMATIC);

    gtk_scrolled_window_set_min_content_height(
        GTK_SCROLLED_WINDOW(log_scroll), 190);

    GtkWidget *log_view = gtk_text_view_new();

    gtk_text_view_set_editable(GTK_TEXT_VIEW(log_view), FALSE);
    gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(log_view), FALSE);
    gtk_text_view_set_wrap_mode(
        GTK_TEXT_VIEW(log_view), GTK_WRAP_WORD_CHAR);

    gtk_widget_add_css_class(log_view, "log-view");

    log_buffer = gtk_text_view_get_buffer(GTK_TEXT_VIEW(log_view));

    gtk_scrolled_window_set_child(
        GTK_SCROLLED_WINDOW(log_scroll), log_view);

    gtk_box_append(GTK_BOX(log_panel), log_scroll);
    gtk_box_append(GTK_BOX(page), log_panel);

    /* Connect button events */
    g_signal_connect(start_button, "clicked",
                     G_CALLBACK(on_start), NULL);

    g_signal_connect(pause_button, "clicked",
                     G_CALLBACK(on_pause), NULL);

    g_signal_connect(resume_button, "clicked",
                     G_CALLBACK(on_resume), NULL);

    g_signal_connect(step_button, "clicked",
                     G_CALLBACK(on_step), NULL);

    g_signal_connect(reset_button, "clicked",
                     G_CALLBACK(on_reset), NULL);

    g_signal_connect(step_check, "toggled",
                     G_CALLBACK(on_step_mode), NULL);

    g_signal_connect(speed_scale, "value-changed",
                     G_CALLBACK(on_speed_changed), NULL);

    gtk_window_present(GTK_WINDOW(window));

    request_refresh();
}

/* ---------- Program entry point ---------- */

int main(int argc, char **argv)
{
    if (sem_init(&resource_sem, 0, 1) != 0) {
        fprintf(stderr, "Failed to initialize resource semaphore.\n");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
        thread_states[i] = g_strdup("IDLE");
        thread_details[i] = g_strdup("Ready to run");
    }

    GtkApplication *app = gtk_application_new(
        "edu.sync-lab.reader-writer",
        G_APPLICATION_DEFAULT_FLAGS);

    g_signal_connect(app, "activate", G_CALLBACK(activate), NULL);

    int status = g_application_run(G_APPLICATION(app), argc, argv);

    g_object_unref(app);
    sem_destroy(&resource_sem);

    for (int i = 0; i < THREAD_COUNT; i++) {
        g_free(thread_states[i]);
        g_free(thread_details[i]);
    }

    return status;
}