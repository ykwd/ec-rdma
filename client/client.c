/*
 * build:
 *   cc -o client client.c -lrdmacm
 *
 * usage:
 *   client <servername> <val1> <val2>
 *
 * connects to server, sends val1 via RDMA write and val2 via send,
 * and receives val1+val2 back from the server.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <infiniband/arch.h>
#include <rdma/rdma_cma.h>

#define DATASIZE (1 << 30)
#define COLUMN 64 
#define ROW 80
#define SERVER 4
#define SENDSIZE (DATASIZE / SERVER)
#define RECVSIZE (DATASIZE / SERVER / COLUMN * ROW)

enum {
	RESOLVE_TIMEOUT_MS	= 5000,
};

struct timeval time_start;

void start() {
        gettimeofday(&time_start,NULL);
}

void end_and_print() {
        struct timeval time_end, res;
        gettimeofday(&time_end,NULL);
        timersub(&time_end,&time_start,&res);

        long long d = (time_end.tv_sec - time_start.tv_sec) * 1000000L + time_end.tv_usec - time_start.tv_usec;
        double dd = d;
        //printf("start %u.%u s\n", time_start.tv_sec, time_start.tv_usec);
        //printf("end %u.%u s\n", time_end.tv_sec, time_end.tv_usec);
        //printf("%u.%u s \n",res.tv_sec,res.tv_usec);
        printf("%.3lf s \n",dd / 1000000);

}

struct pdata {
	uint64_t	buf_va;
	uint32_t	buf_rkey;
};

struct RdmaConn {
	struct rdma_event_channel      *cm_channel;
	struct rdma_cm_id	       *cm_id;
	struct ibv_pd		       *pd;
	struct ibv_comp_channel	       *comp_chan;
	struct ibv_cq		       *cq;
	struct ibv_mr		       *recv_mr;
	struct ibv_mr		       *send_mr;
	char				*recv_buf;
	char 				*send_buf;	
	struct pdata			server_pdata;		
};

struct RdmaConn* my_connect(const char* server)
{
	struct pdata			server_pdata;

	struct rdma_event_channel      *cm_channel;
	struct rdma_cm_id	       *cm_id;
	struct rdma_cm_event	       *event;
	struct rdma_conn_param		conn_param = { };

	struct ibv_pd		       *pd;
	struct ibv_comp_channel	       *comp_chan;
	struct ibv_cq		       *cq;
	struct ibv_mr		       *recv_mr;
	struct ibv_mr		       *send_mr;
	struct ibv_qp_init_attr		qp_attr = { };

	struct addrinfo		       *res, *t;
	struct addrinfo			hints = {
		.ai_family   = AF_INET,
		.ai_socktype = SOCK_STREAM
	};
	int				n;

	char				*recv_buf;
	char 				*send_buf;	

	int				err;

	struct RdmaConn			*rdma_conn = NULL;
	/* Set up RDMA CM structures */

	cm_channel = rdma_create_event_channel();
	if (!cm_channel)
		return NULL;

	err = rdma_create_id(cm_channel, &cm_id, NULL, RDMA_PS_TCP);
	if (err)
		return NULL;

	n = getaddrinfo(server, "20079", &hints, &res);
	if (n < 0)
		return NULL;

	/* Resolve server address and route */

	for (t = res; t; t = t->ai_next) {
		err = rdma_resolve_addr(cm_id, NULL, t->ai_addr,
					RESOLVE_TIMEOUT_MS);
		if (!err)
			break;
	}
	if (err)
		return NULL;

	err = rdma_get_cm_event(cm_channel, &event);
	if (err)
		return NULL;

	if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
		return NULL;

	rdma_ack_cm_event(event);

	err = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS);
	if (err)
		return NULL;

	err = rdma_get_cm_event(cm_channel, &event);
	if (err)
		return NULL;

	if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
		return NULL;

	rdma_ack_cm_event(event);

	/* Create verbs objects now that we know which device to use */

	pd = ibv_alloc_pd(cm_id->verbs);
	if (!pd)
		return NULL;

	comp_chan = ibv_create_comp_channel(cm_id->verbs);
	if (!comp_chan)
		return NULL;

	cq = ibv_create_cq(cm_id->verbs, 2, NULL, comp_chan, 0);
	if (!cq)
		return NULL;

	if (ibv_req_notify_cq(cq, 0))
		return NULL;

	send_buf = (char*) malloc(SENDSIZE);
	recv_buf = (char*) malloc(RECVSIZE);

	recv_mr = ibv_reg_mr(pd, recv_buf, RECVSIZE ,
			IBV_ACCESS_LOCAL_WRITE |
			IBV_ACCESS_REMOTE_READ |
			IBV_ACCESS_REMOTE_WRITE);
	if (!recv_mr)
		return NULL;
	send_mr = ibv_reg_mr(pd, send_buf, SENDSIZE,
			IBV_ACCESS_LOCAL_WRITE |
			IBV_ACCESS_REMOTE_READ |
			IBV_ACCESS_REMOTE_WRITE);
	if (!send_mr)
		return NULL;

	qp_attr.cap.max_send_wr	 = 2;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_wr	 = 1;
	qp_attr.cap.max_recv_sge = 1;

	qp_attr.send_cq		 = cq;
	qp_attr.recv_cq		 = cq;

	qp_attr.qp_type		 = IBV_QPT_RC;

	err = rdma_create_qp(cm_id, pd, &qp_attr);
	if (err)
		return NULL;

	conn_param.initiator_depth = 1;
	conn_param.retry_count	   = 7;

	/* Connect to server */

	err = rdma_connect(cm_id, &conn_param);
	if (err)
		return NULL;

	err = rdma_get_cm_event(cm_channel, &event);
	if (err)
		return NULL;

	if (event->event != RDMA_CM_EVENT_ESTABLISHED)
		return NULL;

	memcpy(&server_pdata, event->param.conn.private_data,
	       sizeof server_pdata);

	rdma_ack_cm_event(event);

	rdma_conn = (struct RdmaConn*) malloc(sizeof(struct RdmaConn));
	rdma_conn->cm_channel = cm_channel;
	rdma_conn->cm_id = cm_id;
	rdma_conn->pd = pd;
	rdma_conn->comp_chan = comp_chan;
	rdma_conn->cq = cq;
	rdma_conn->recv_mr = recv_mr;
	rdma_conn->send_mr = send_mr;
	rdma_conn->recv_buf = recv_buf;
	rdma_conn->send_buf = send_buf;
	memcpy(&rdma_conn->server_pdata, &server_pdata, sizeof server_pdata);
	return rdma_conn;
}

int work(struct RdmaConn *conn)
{
	struct ibv_sge			sge;
	struct ibv_send_wr		send_wr = { };
	struct ibv_send_wr	       *bad_send_wr;
	struct ibv_recv_wr		recv_wr = { };
	struct ibv_recv_wr	       *bad_recv_wr;
	struct ibv_wc			wc;
	void			       *cq_context;
	struct ibv_cq			*evt_cq;

	int				err;
	int 				n;

	struct rdma_event_channel      *cm_channel = conn->cm_channel;
	struct rdma_cm_id	       *cm_id = conn->cm_id;
	struct ibv_pd		       *pd = conn->pd;
	struct ibv_comp_channel	       *comp_chan = conn->comp_chan;
	struct ibv_cq		       *cq = conn->cq;
	struct ibv_mr		       *recv_mr = conn->recv_mr;
	struct ibv_mr		       *send_mr = conn->send_mr;
	char				*recv_buf = conn->recv_buf;
	char 				*send_buf = conn->send_buf;	
	struct pdata			server_pdata;
	memcpy(&server_pdata, &conn->server_pdata, sizeof(struct pdata));

	/* Prepost receive */

	sge.addr   = recv_buf;
	sge.length = RECVSIZE;
	sge.lkey   = recv_mr->lkey;

	recv_wr.wr_id   = 0;
	recv_wr.sg_list = &sge;
	recv_wr.num_sge = 1;

	if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr))
		return 1;

	/* Write/send two integers to be added */

	sge.addr   = send_buf;
	sge.length = SENDSIZE;
	sge.lkey   = send_mr->lkey;

	send_wr.wr_id		    = 1;
	send_wr.opcode		    = IBV_WR_SEND;
	send_wr.send_flags	    = IBV_SEND_SIGNALED;
	send_wr.sg_list		    = &sge;
	send_wr.num_sge		    = 1;
	send_wr.wr.rdma.rkey	    = ntohl(server_pdata.buf_rkey);
	send_wr.wr.rdma.remote_addr = ntohll(server_pdata.buf_va);

	if (ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr))
		return 1;

	/* Wait for receive completion */

	while (1) {
		if (ibv_get_cq_event(comp_chan, &evt_cq, &cq_context))
			return 1;

		if (ibv_req_notify_cq(cq, 0))
			return 1;

		while ((n = ibv_poll_cq(cq, 1, &wc)) > 0) {
			if (wc.status != IBV_WC_SUCCESS)
				return 1;

			if (wc.wr_id == 0) {
				puts("success");
				ibv_ack_cq_events(cq, 2);
				return 0;
			}
		}

		if (n < 0)
			return 1;
	}
}

void* pwork(void *param)
{
	work((struct RdmaConn*) param);
}

int main(int argc, char *argv[])
{
	char servers[4][20];
	struct RdmaConn** conns = malloc(SERVER * sizeof(struct RdmaConn*));
	int i;
	pthread_t *threads;
	strcpy(servers[0], "10.0.0.6");
	strcpy(servers[1], "10.0.0.7");
	strcpy(servers[2], "10.0.0.8");
	strcpy(servers[3], "10.0.0.9");
	for (i = 0; i < SERVER; i++) {
		conns[i] = my_connect(servers[i]);
	}
	if (argc > 1 && 0 == strcmp(argv[1], "p")) {
		// parallel execution
		start();
		threads = malloc(sizeof(pthread_t)*SERVER);
		for (i = 0; i < SERVER; i++) {
			pthread_create(threads+i, NULL, pwork, (void *)(conns[i]));
		}
		for (i = 0; i < SERVER; i++)
			pthread_join(threads[i], NULL);
		free(threads);
		end_and_print();
	} else {
		//pipe line execution
		
	}
	return 0;
}
