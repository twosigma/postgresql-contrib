/*
 * Copyright (c) 2016 Two Sigma Open Source, LLC.
 * All Rights Reserved
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL TWO SIGMA OPEN SOURCE, LLC BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF TWO SIGMA OPEN SOURCE, LLC HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * TWO SIGMA OPEN SOURCE, LLC SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
 * BASIS, AND TWO SIGMA OPEN SOURCE, LLC HAS NO OBLIGATIONS TO PROVIDE
 * MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

/*
 * Based on src/test/examples/testlibpq2.c from Postgres 9.4.4
 *
 * pqasyncnotifier - LISTENs and reports notifications without polling
 *
 * Usage: pqasyncnotifier CONNINFO TABLE_NAME
 */

#ifdef WIN32
#include <windows.h>
#endif

#define _GNU_SOURCE
#include <sys/time.h>
#include <sys/types.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <libpq-fe.h>

static void
err(PGconn *conn, ConnStatusType rcc, ExecStatusType rce, const char *msg)
{
    int e = errno;

    fprintf(stderr, "Error: %s: ", msg);
    if (conn == NULL || (rcc == CONNECTION_OK && rce == PGRES_COMMAND_OK))
        fprintf(stderr, "%s", strerror(e));
    else if (conn != NULL && (rcc != CONNECTION_OK || rce != PGRES_COMMAND_OK))
        fprintf(stderr, "%s", PQerrorMessage(conn));
    else
        fprintf(stderr, "Unknown error");
    fprintf(stderr, "\n");
    if (conn)
        PQfinish(conn);
    exit(e ? e : 1);
}

static struct option lopts[] = {
    {"help", 0, 0, 'h'},
    {"verbose", 0, 0, 'v'},
    {"daemonize", 0, 0, 'D'},
    {"include-payload", 0, 0, 'd'},
    {"include-pid", 0, 0, 'p'},
    {"include-time", 0, 0, 't'},
    {"include-channel-name", 0, 0, 'c'},
};

static void
usage(const char *prog, int e)
{
    size_t i;

    if (strrchr(prog, '/') != NULL && strrchr(prog, '/')[1] != '\0')
        prog = strrchr(prog, '/') + 1;

    fprintf(stderr, "Usage: %s [options] CONNINFO TABLE-NAME ...\n", prog);
    fprintf(stderr, "    Options:\n");
    for (i = 0; i < sizeof(lopts)/sizeof(lopts[0]); i++)
        fprintf(stderr, "\t-%c, --%s\n", lopts[i].val, lopts[i].name);
    exit(e);
}

static
void
daemonize(int ready)
{
    static int pipe_fds[2] = {-1, -1};
    pid_t pid;
    char code = 1;

    if (ready) {
        errno = 0;
        if (pipe_fds[1] != -1) {
            while (write(pipe_fds[1], "", sizeof("")) != 1 && errno != EINTR)
                ;
            if (errno != 0)
                err(NULL, 0, 0, "write() failed while daemonizing");
            if (close(pipe_fds[1]) != 0)
                err(NULL, 0, 0, "close() failed while daemonizing");
            pipe_fds[1] = -1;
        }
        printf("READY: %jd\n", (intmax_t)getpid());
        return;
    }

    if (pipe(pipe_fds) == -1)
        err(NULL, 0, 0, "pipe() failed");
    pid = fork();
    if (pid == -1)
        err(NULL, 0, 0, "fork() failed");
    if (pid == 0) {
        (void) close(pipe_fds[0]);
        pipe_fds[0] = -1;
        return;
    }
    (void) close(pipe_fds[1]);
    pipe_fds[1] = -1;
    while (read(pipe_fds[0], &code, sizeof(code)) != 1 && errno != EINTR)
        ;
    _exit(code);
}

int
main(int argc, char **argv)
{
    const char          *prog = argv[0];
    const char          *conninfo;
    PGconn              *conn = NULL;
    PGresult            *res;
    PGnotify            *notify;
    ConnStatusType      rcc = CONNECTION_OK;
    ExecStatusType      rce = PGRES_COMMAND_OK;
    int                 include_relname = 0;
    int                 include_payload = 0;
    int                 include_pid = 0;
    int                 include_time = 0;
    int                 verbose = 0;
    char                c;

    setlinebuf(stdout);

    while ((c = getopt_long(argc, argv, "+Dcdhptv", lopts, NULL)) != -1) {
        switch (c) {
        case 'D':
            daemonize(0);
        case 'c':
            include_relname = 1;
            break;
        case 'd':
            include_payload = 1;
            break;
        case 'h':
            usage(prog, 0);
            break;
        case 'p':
            include_pid = 1;
            break;
        case 't':
            include_time = 1;
            break;
        case 'v':
            verbose = 1;
            break;
        default:
            usage(prog, 1);
            break;
        }
    }

    argc -= optind;
    argv += optind;

    if (argc < 2)
        usage(prog, 1);

    conninfo = argv[0];

    argc--;
    argv++;

    conn = PQconnectdb(conninfo);
    if (conn == NULL || (rcc = PQstatus(conn)) != CONNECTION_OK)
        err(conn, rcc, rce, "Connection to database failed");

    for (; argc > 0; argc--, argv++) {
        char *listen_cmd;

        if (asprintf(&listen_cmd, "LISTEN %s", argv[0]) == -1 ||
            listen_cmd == NULL)
            err(conn, rcc, rce, "Could not format SQL statement");

        if (verbose)
            printf("Executing %s\n", listen_cmd);

        res = PQexec(conn, listen_cmd);
        free(listen_cmd);
        if (res == NULL)
            err(conn, rcc, rce, "PQexec failed (out of memory?)");

        rce = PQresultStatus(res);
        PQclear(res);
        if (rce != PGRES_COMMAND_OK)
            err(conn, rcc, rce, "LISTEN command failed");
    }

    daemonize(1);

    for (;;) {
        int     sock;
        fd_set  input_mask;

        sock = PQsocket(conn);
        if (sock < 0)
            err(conn, rcc, rce, "PQsocket failed");

        FD_ZERO(&input_mask);
        FD_SET(sock, &input_mask);

        if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
            err(conn, rcc, rce, "select() failed");

        PQconsumeInput(conn);
        while ((notify = PQnotifies(conn)) != NULL) {
            if (include_time)
                printf("%jd ", (long)time(NULL));
            printf("NOTIFY");
            if (include_relname)
                printf(" %s", notify->relname);
            if (include_pid)
                printf(" %d", notify->be_pid);
            if (include_payload)
                printf(": %s", notify->extra);
            printf("\n");
            PQfreemem(notify);
        }
    }

    PQfinish(conn);
    return 0;
}

