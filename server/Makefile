CFLAGS += -O2

server: server.o ec-method.o ec-gf.o -lrdmacm -libverbs -lpthread

server.o: server.c ec-method.h

ec-method.o: ec-method.c ec-method.h ec-gf.h 

ec-gf.o: ec-gf.c ec-gf.h

clean:
	$(RM) server server.o ec-method.o ec-gf.o
