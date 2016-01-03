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
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <infiniband/arch.h>
#include <rdma/rdma_cma.h>

#define DATASIZE (1 << 30)
#define COLUMN 16
#define ROW 24
#define SERVER 4
#define SENDSIZE (DATASIZE / SERVER)
#define RECVSIZE (DATASIZE / SERVER / COLUMN * ROW)

//char** servers = {"10.0.0.6", "10.0.0.7"};
char servers[20][20];

enum {
	RESOLVE_TIMEOUT_MS	= 5000,
};

struct pdata {
	uint64_t	buf_va;
	uint32_t	buf_rkey;
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

struct Param {
	struct rdma_event_channel 	*cm_channel; 
	struct rdma_cm_id		*cm_id;
	struct ibv_pd			*pd;			
	struct ibv_mr		       *recv_mr;
	struct ibv_mr		       *send_mr;
	char 				*recv_buf;
	char 				*send_buf;
};


struct rdma_event_channel      **cm_channel;
struct rdma_cm_id	       **cm_id;
struct rdma_cm_event	       **event;

struct ibv_pd		       **pd;
struct ibv_mr		       **recv_mr;
struct ibv_mr		       **send_mr;

struct addrinfo		       **res, **t;
struct addrinfo			**hints;
struct addrinfo			hints_temp = {
	.ai_family   = AF_INET,
	.ai_socktype = SOCK_STREAM
};

char				**send_buf;
char				**recv_buf;

void* mainrun(void* param);

int main(int argc, char *argv[])
{
	int n, i ,err;
	pthread_t *threads;
	struct Param *params;
	strcpy(servers[0], "10.0.0.6");
	strcpy(servers[1], "10.0.0.7");
	strcpy(servers[2], "10.0.0.8");
	strcpy(servers[3], "10.0.0.9");
	send_buf = (char**) malloc(SERVER * sizeof(char*));
	recv_buf = (char**) malloc(SERVER * sizeof(char*));
	send_mr = (struct ibv_mr**) malloc(SERVER * sizeof(struct ibv_mr*));
	recv_mr = (struct ibv_mr**) malloc(SERVER * sizeof(struct ibv_mr*));
	cm_channel = (struct rdma_event_channel**) malloc(SERVER * sizeof(struct rdma_event_channel*));
	cm_id = (struct rdma_cm_id**) malloc(SERVER * sizeof(struct rdma_cm_id*));
	event = (struct rdma_cm_event**) malloc(SERVER * sizeof(struct rdma_cm_event*));
	res = (struct addrinfo **) malloc(SERVER * sizeof(struct addrinfo*));
	t = (struct addrinfo **) malloc(SERVER * sizeof(struct t*));
	hints = (struct addrinfo **) malloc(SERVER * sizeof(struct t*));
	pd = (struct ibv_pd **) malloc(SERVER * sizeof(struct ibv_pd*));
	
	for (i = 0; i < SERVER; i++) {
		send_buf[i] = (char*) malloc(SENDSIZE);
		recv_buf[i] = (char*) malloc(RECVSIZE);
		send_mr[i] = NULL;
		recv_mr[i] = NULL;
		hints[i] = (struct addrinfo*) malloc (sizeof(struct addrinfo));
		memcpy(hints[i], &hints_temp, sizeof(struct addrinfo));

		/* Set up RDMA CM structures */

		cm_channel[i] = rdma_create_event_channel();
		if (!cm_channel)
			return 1;

		err = rdma_create_id(cm_channel[i], &cm_id[i], NULL, RDMA_PS_TCP);
		if (err)
			return err;

		n = getaddrinfo(servers[i], "20079", hints[i], &res[i]);
		if (n < 0)
			return 1;

		/* Resolve server address and route */

		for (t[i] = res[i]; t[i]; t[i] = t[i]->ai_next) {
			err = rdma_resolve_addr(cm_id[i], NULL, t[i]->ai_addr,
						RESOLVE_TIMEOUT_MS);
			if (!err)
				break;
		}
		if (err)
			return err;

		err = rdma_get_cm_event(cm_channel[i], &event[i]);
		if (err)
			return err;

		if (event[i]->event != RDMA_CM_EVENT_ADDR_RESOLVED)
			return 1;

		rdma_ack_cm_event(event[i]);

		err = rdma_resolve_route(cm_id[i], RESOLVE_TIMEOUT_MS);
		if (err)
			return err;

		err = rdma_get_cm_event(cm_channel[i], &event[i]);
		if (err)
			return err;

		if (event[i]->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
			return 1;

		rdma_ack_cm_event(event[i]);

		pd[i] = ibv_alloc_pd(cm_id[i]->verbs);
		if (!pd)
			return 1;

		recv_mr[i] = ibv_reg_mr(pd[i], recv_buf[i], RECVSIZE ,
				IBV_ACCESS_LOCAL_WRITE |
				IBV_ACCESS_REMOTE_READ |
				IBV_ACCESS_REMOTE_WRITE);
		if (!recv_mr[i])
			return 1;
		send_mr[i] = ibv_reg_mr(pd[i], send_buf[i], SENDSIZE,
				IBV_ACCESS_LOCAL_WRITE |
				IBV_ACCESS_REMOTE_READ |
				IBV_ACCESS_REMOTE_WRITE);
		if (!send_mr[i])
			return 1;
	}

	start();
	threads = malloc(sizeof(pthread_t)*SERVER);
	params = malloc(sizeof(struct Param)*SERVER);
	for (i = 0; i < SERVER; i++) {
		params[i] = (struct Param){
			.cm_channel = cm_channel[i],
			.cm_id = cm_id[i],
			.pd = pd[i],
			.recv_mr = recv_mr[i],
			.send_mr = send_mr[i],
			.recv_buf = recv_buf[i],
			.send_buf = send_buf[i],
		};
		pthread_create(threads+i,NULL,mainrun,(void *)(params+i));
	}
	for (i = 0; i < SERVER; i++) 
		pthread_join(threads[i], NULL);
	free(threads);
	free(params);
	end_and_print();
	return 0;
}

void *mainrun(void* param) {
	struct Param *p = (struct Param*)param;
	int r;
	r = run(p->cm_channel, p->cm_id, p->pd, p->recv_mr, p->send_mr, p->recv_buf, p->send_buf);
	if (r) puts("fail");
	else puts("success");
}
	
int run(struct rdma_event_channel 	*cm_channel, 
	struct rdma_cm_id		*cm_id,
	struct ibv_pd			*pd,			
	struct ibv_mr		       *recv_mr,
	struct ibv_mr		       *send_mr,
	char 				*recv_buf,
	char 				*send_buf) {

	struct pdata			server_pdata;

	struct rdma_conn_param		conn_param = { };
	struct rdma_cm_event	       *event;
	struct ibv_comp_channel	       *comp_chan;
	struct ibv_cq		       *cq;
	struct ibv_cq		       *evt_cq;

	struct ibv_qp_init_attr		qp_attr = { };
	struct ibv_sge			sge;
	struct ibv_send_wr		send_wr = { };
	struct ibv_send_wr	       *bad_send_wr;
	struct ibv_recv_wr		recv_wr = { };
	struct ibv_recv_wr	       *bad_recv_wr;
	struct ibv_wc			wc;
	void			       *cq_context;

	int				n;
	int 				err;

	struct rdma_cm_event		temp_event;
	/* Create verbs objects now that we know which device to use */

	puts("create comp channel");
	comp_chan = ibv_create_comp_channel(cm_id->verbs);
	if (!comp_chan)
		return 1;

	puts("create cq");
	cq = ibv_create_cq(cm_id->verbs, 2, NULL, comp_chan, 0);
	if (!cq)
		return 1;

	puts("notify cq");
	if (ibv_req_notify_cq(cq, 0))
		return 1;

	qp_attr.cap.max_send_wr	 = 2;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_wr	 = 1;
	qp_attr.cap.max_recv_sge = 1;

	qp_attr.send_cq		 = cq;
	qp_attr.recv_cq		 = cq;

	qp_attr.qp_type		 = IBV_QPT_RC;

	puts("create qp");
	err = rdma_create_qp(cm_id, pd, &qp_attr);
	if (err)
		return err;

	conn_param.initiator_depth = 1;
	conn_param.retry_count	   = 7;

	/* Connect to server */

	puts("rdma_connect");
	err = rdma_connect(cm_id, &conn_param);
	printf("%d\n", err);
	if (err)
		return err;

	puts("rdma_get_cm_event");
	err = rdma_get_cm_event(cm_channel, &event);
	if (err)
		return err;

	if (event->event != RDMA_CM_EVENT_ESTABLISHED)
		return 1;

	memcpy(&server_pdata, event->param.conn.private_data,
	       sizeof server_pdata);
	memcpy(&temp_event, event, sizeof(temp_event));

	rdma_ack_cm_event(event);

	/* Prepost receive */
again:
	sge.addr   = recv_buf;
	sge.length = RECVSIZE;
	sge.lkey   = recv_mr->lkey;

	recv_wr.wr_id   = 0;
	recv_wr.sg_list = &sge;
	recv_wr.num_sge = 1;

	puts("post_recv");
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
//				puts("success");
				goto end;
				return 0;
			}
		}

		if (n < 0)
			return 1;
	}
end:
	rdma_disconnect(cm_id);
	rdma_destroy_qp(temp_event.id);
	puts("destroyed qp");

	return 0;
}
