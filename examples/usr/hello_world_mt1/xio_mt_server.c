/*
*
 * Copyright (c) 2013 Mellanox Technologies®. All rights reserved.
 *
 * This software is available to you under a choice of one of two licenses.
 * You may choose to be licensed under the terms of the GNU General Public
 * License (GPL) Version 2, available from the file COPYING in the main
 * directory of this source tree, or the Mellanox Technologies® BSD license
 * below:
 *
 *      - Redistribution and use in source and binary forms, with or without
 *        modification, are permitted provided that the following conditions
 *        are met:
 *
 *      - Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 *      - Neither the name of the Mellanox Technologies® nor the names of its
 *        contributors may be used to endorse or promote products derived from
 *        this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <dirent.h>
#include <memory.h>
#include <unistd.h>
#include <errno.h>

#include "libxio.h"

#define MAX_THREADS 	6	
#define QUEUE_DEPTH	        3	
#define PRINT_COUNTER		1000000
#define DISCONNECT_NR		2000000

int test_disconnect;

struct portals_vec {
	int			vec_len;
	int			pad;
	const char		*vec[MAX_THREADS];
};

struct thread_data {
	char			portal[64];
	int			affinity;
	int			cnt;
	int			nsent;
	int			pad;
	struct xio_msg		rsp[QUEUE_DEPTH];
	struct xio_context	*ctx;
	struct xio_connection	*connection;
	pthread_t		thread_id;
};


/* server private data */
struct server_data {
	struct xio_context	*ctx;
	struct thread_data	tdata[MAX_THREADS];
};

/*-------------------------------*/
/* Tachyon API implementation    */
/*-------------------------------*/

/* file or dir exist  */
int file_exist (const char *filename)
{
  struct stat   buffer;
  return (stat (filename, &buffer) == 0);
}


int  Is_File(const char *path)
{
  int status;
  struct stat st_buf;

    status = stat (path, &st_buf);
    if (status != 0) {
        printf ("Error, errno = %d  %s\n", errno, path);
        return 0;
    }

    if (S_ISREG(st_buf.st_mode)) {
        printf ("%s is a regular file.\n", path);
        return (1);
    }
    return (0);
}




int  Is_Directory(const char *path)
{
  int status;
  struct stat st_buf;
    status = stat (path, &st_buf);
    if (status != 0) {
        printf ("Error, errno = %d\n", errno);
        return (0);
    }


    if (S_ISDIR(st_buf.st_mode)) {
        printf ("%s is a directory.\n", path);
        return (1);
    }
    return (0);
}

int remove_directory_files(const char *path)
{
   DIR *d = opendir(path);
   size_t path_len = strlen(path);
   int r = -1;

   if (d)
   {
      struct dirent *p;

      r = 0;

      while (!r && (p=readdir(d)))
      {
          int r2 = -1;
          char *buf;
          size_t len;

          /* Skip the names "." and ".." as we don't want to recurse on them. */
          if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
          {
             continue;
          }

          len = path_len + strlen(p->d_name) + 2;
          buf = (char *)malloc(len);

          if (buf)
          {
             struct stat statbuf;

            snprintf(buf, len, "%s/%s", path, p->d_name);

             if (!stat(buf, &statbuf))
             {
                if (S_ISDIR(statbuf.st_mode))
                {
                   r2 = remove_directory_files(buf);
                }
                else
                {
                   r2 = unlink(buf);
                }
             }

             free(buf);
          }

          r = r2;
      }
   }
   return (r == 0)? 1:0;
}

int list_directory(const char *path, char *ret_data, size_t *ret_len)
{
   DIR *d = opendir(path);
   char *buf[20000];
   int num_files =0;
   int total_len =0;
   int i =0;

   printf(" path is: %s,  pointer d is %p \n", path,d);

   if (d)
   {
      struct dirent *p;

      while ((p=readdir(d)))
      {
          size_t len;

          /* Skip the names "." and ".." as we don't want to recurse on them. */
          if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
          {
             continue;
          }
          printf ("d_name is: %s\n", p->d_name);

          len =  strlen(p->d_name) + 2;
           buf[num_files] = (char *)malloc(len);

          if (buf[num_files])
          {
             snprintf(buf[num_files], len, "%s ",  p->d_name);
             num_files ++;
             total_len = total_len + len + 1;   //add one for space 
          }

      }
      if (num_files  > 0) {
        sprintf(ret_data,"status=%d response=%d\n", 1, num_files);
        for (i=0; i < num_files; i++) {
          strcat(ret_data, buf[i]);
          free(buf[i]);
        }
      }
      *ret_len=total_len;
      printf("list array is:%s  %p  %ld\n", ret_data, ret_data, *ret_len); 
  }
 return (num_files);
}

/* to set permissions */
int set_permissions (const char *path, mode_t mode)
{
   return ( chmod(path,  mode));
}

FILE* create_file(const char *path)
{
   FILE *fptr;
   int ret;
   fptr = fopen(path, "w");
   printf("creat fptr: %p \n", fptr);
   if (fptr != NULL)
   {
       ret= set_permissions (path, 01777);
       printf("ret from permissions %d \n",  ret);
       if (ret)
       {
          fptr=NULL;
          fclose(fptr);
       }
   }
   return fptr;
}

int close_file(FILE*  fptr)
{
   int ret;

     ret = fclose(fptr);
     return ret;
}

int deleteobj (const char *path, int  recursive)
{
  int  success=1;
     if (recursive && Is_Directory(path))
        remove_directory_files(path);
     return (success && remove(path))? 1:0;
}

long getBlockSizeByte(const char *path)
{
   if ( file_exist(path) )
     return (1024 *1024 *128L); /*just return 128MB */
   else
   return (-1);
}

long getFileSize(const char *path)
{
  int status;
  struct stat st_buf;

    status = stat(path, &st_buf);
    printf("get file status: %d  size: %ld\n", status, st_buf.st_size);
    if (status != 0) {
        printf ("Error, errno = %d\n", errno);
        return 0;
    }

    return (st_buf.st_size);
}

long getModificationTimeMs(const char *path)
{
  int status;
  struct stat st_buf;

    status = stat (path, &st_buf);
    if (status != 0) {
        printf ("Error, errno = %d\n", errno);
        return 0;
    }

    return (st_buf.st_mtime);
}


/* temp definition */
#define SPACE_TOTAL 0
#define SPACE_FREE  1
#define SPACE_USED  2

long getSpace(const char *path, int type)
{
  int status;
  struct statvfs st_buf;

    status = statvfs (path, &st_buf);
    if (status != 0) {
        printf ("Error, errno = %d\n", errno);
        return 0;
    }
    
    switch (type) {
       case SPACE_TOTAL:
            return( st_buf.f_blocks * st_buf.f_frsize);
       case SPACE_FREE:
            return( st_buf.f_bavail * st_buf.f_bsize);
       case SPACE_USED:
            return(( st_buf.f_blocks * st_buf.f_frsize) -( st_buf.f_bavail * st_buf.f_bsize)) ;
       default:
            return (-1);
    }
}


int mkdirs(const char * path, int createParent)
{
 int ret;
 if (createParent)
     ret= system(strcat("mkdir -p", path) );
 else
     ret=mkdir(path, 0777);   
 if (ret !=0)
     return (0);
 set_permissions (path, 01777);
 return (1);  /*created */       
}

FILE* open_file(const char *path)
{
   FILE *fptr;
   fptr = fopen(path, "r");
   printf("open fptr: %p \n", fptr);
   return fptr;
}

int flush_file(FILE * fptr)
{
   return fflush(fptr);
}

int rename_file (const char * old_file, const char *new_file)
{
 int ret;
     ret = rename(old_file, new_file);
     printf("in method %s %s\n", old_file, new_file);
     return((ret)?0:1);
}
 

int write_file (FILE* fptr, int offset, size_t size, const void *data_buf, size_t data_buflen )
{
 size_t bytes_written=0;
 int ret=0;
     printf("inside write_file stream %p offset=%d size=%ld  bptr=%p len=%ld\n", fptr, offset, size, data_buf, data_buflen);
     if (size > data_buflen)
        return (-1);
     if (offset !=0)
        ret = fseek( fptr, offset, SEEK_CUR);
     if (ret)
        return (-1);
     bytes_written =  fwrite(data_buf, size, 1, fptr );
     printf("bytes written:%ld len: %ld  \n", bytes_written , data_buflen);
     return(( bytes_written == 1)?1:0);
}

int read_file (FILE* fptr, int offset, size_t size, char *ret_data, size_t *ret_len )
{
 size_t bytes_read=0;
 size_t sr_size;
 int ret=0;
     if (offset !=0)
        ret = fseek( fptr, offset, SEEK_CUR);
     if (ret)
          return (-1);

     sprintf(ret_data, "status=1 response=read \n");
     sr_size= strlen ((char*)ret_data);
    
     bytes_read =  fread(&ret_data[sr_size], size, 1, fptr );
     printf("bytes read:%ld \n", bytes_read);
     *ret_len = sr_size + size;
     if ( bytes_read== 1)
       return 1;  //all done
     else
       sprintf(ret_data, "status=0 response=read \n");
     return  0;
}








static struct portals_vec *portals_get(struct server_data *server_data,
				       const char *uri, void *user_context)
{
	/* fill portals array and return it. */
	 static int		i;
	struct portals_vec	*portals = (struct portals_vec *)
						calloc(1, sizeof(*portals));
	for (i = 0; i < MAX_THREADS; i++) {
		portals->vec[i] = strdup(server_data->tdata[i].portal);
		portals->vec_len++;
	}

	return portals;
}

static void portals_free(struct portals_vec *portals)
{
	int			i;
	for (i = 0; i < portals->vec_len; i++)
		free((char *)(portals->vec[i]));

	free(portals);
}

#define RENAME   "rename"
#define FL_EXIST "fexists"
#define IS_FILE  "isfile" 
#define IS_DIR   "isdir" 
#define RM_DIR   "rmdir"
#define LIST_DIR "listdir"
#define DELETE   "delete"
#define GET_BLK_SZ "getblocksz"
#define GET_FL_SZ  "get_fl_sz"
#define GET_M_TIME  "get_m_time"
#define GET_SPACE   "get_space"
#define CREATE      "create"
#define MK_DIRS   "mkdirs"
#define SET_PERM  "setperm"
#define OPEN        "open"
#define CLOSE       "close"
#define SWRITE      "swrite"
#define SFLUSH      "sflush"
#define SREAD       "sread"


/*---------------------------------------------------------------------------*/
/* process_3par_request                                                           */
/*---------------------------------------------------------------------------*/
static void process_3par_request(struct thread_data *tdata,
                            struct xio_msg *req)
{
       int nparms;
       char mycmd[10];
       char myparm1[200];
       char myparm2[200];
       char myparm3[200];
       long  status=0;
       int td=0;
       void * myptr = NULL;
       //char  response_extension[80] =" test of response";
       char  response_extension[80] =" response=0 ";
       struct xio_iovec_ex     *sglist = vmsg_sglist(&req->in);
       nparms =0;
       td= req->sn %QUEUE_DEPTH ;
       td=0; //bob temp
       //tdata->rsp[td].out.data_iov.sglist[0].iov_len=0;   //flag for global copy os status response  non-zero means no update as was set in cmd already
       printf(" in 3par request\n"); 
       printf("thread_affinity: %d   id:%pi connection:%p\n", tdata->affinity,(char *)tdata->thread_id, tdata->connection);
       //printf ("data is[%ld]:%s\n", sglist[0].iov_len , (char *)sglist[0].iov_base); 
       //printf ("data is:%s\n", (char *)req->in.header.iov_base); 
       nparms=sscanf((char *)sglist[0].iov_base,"cmd=%s parm1=%s parm2=%s parm3=%s\n",mycmd,myparm1, myparm2, myparm3);
       //nparms=sscanf((char *)req->in.header.iov_base,"cmd=%s parm1=%s ",mycmd,myparm1);
       printf("nparms: %d\n", nparms);
       
       if (nparms > 0) 
       {
             tdata->rsp[td].out.data_iov.sglist[0].iov_len=0;
	     printf("**********************************************************\n");
             printf("*** vlal: %s %s %s %s td[%d]\n", mycmd,myparm1, myparm2, myparm3, td);
	     printf("**********************************************************\n");
             sprintf(response_extension,"%s", "response=none"); //initialize for no updates calls;
             if (strcmp (mycmd, OPEN) ==0)
             {
                  FILE * fd; 
                  fd = open_file(myparm1);
                  sprintf (response_extension,"%s%p ","response=", fd);
                  (fd == NULL)? (status = -1) : (status =0);

             }
             if (strcmp (mycmd, CLOSE) ==0)
             {
                  void * fptr;
                  sscanf(myparm1, "%p", &fptr);
                  status = close_file((FILE *)fptr);
                  sprintf (response_extension,"%s ","response=fclosedone");  
             }

             if (strcmp (mycmd, RENAME) ==0)
             {
                  status = rename_file(myparm1,myparm2);
             }
             if (strcmp (mycmd,CREATE) == 0 )
             {
                  myptr = create_file( (const char *) myparm1); 
             /*     sprintf ((char *)tdata->rsp[td].out.header.iov_base,"%s%p","response=", myptr);
                  tdata->rsp[td].out.header.iov_len = 
                          strlen((const char *)tdata->rsp[td].out.header.iov_base) + 1; 
                  printf("create %s  is it[%d]\n", (char *)tdata->rsp[td].in.header.iov_base, td);
             */
                  printf("create fptr:%p\n", myptr);
                  sprintf (response_extension,"%s%p ","response=", myptr);   
                  (myptr == NULL)? (status = -1) : (status =0);
                  
             }

             if (strcmp (mycmd,DELETE) ==0)
             {
                  status = deleteobj(myparm1,atoi(myparm2));
             }

             if (strcmp (mycmd, FL_EXIST) ==0)
             {
                  status = Is_File(myparm1);
                  printf ("status from fexist: %ld\n", status);
                  sprintf (response_extension,"%s ","response=exists");
             }

             if (strcmp (mycmd,GET_BLK_SZ) ==0)
             {
                  status = getBlockSizeByte(myparm1); 
             }

             if (strcmp (mycmd,GET_FL_SZ) ==0)
             {
                  status = getFileSize(myparm1);
             }

             if (strcmp (mycmd,GET_M_TIME) ==0)
             {
                  status = getModificationTimeMs(myparm1);
             }          
             if (strcmp (mycmd,GET_SPACE) ==0)
             {
                  status = getSpace(myparm1, atoi(myparm2));
             }
             if (strcmp (mycmd,IS_FILE) ==0)
             {
                  status = Is_File(myparm1);
             }
             if (strcmp (mycmd,LIST_DIR) ==0)
             {
                  status = list_directory(myparm1, (char *)tdata->rsp[td].out.data_iov.sglist[0].iov_base, &tdata->rsp[td].out.data_iov.sglist[0].iov_len );
                  printf ("list_data is: %s len=%ld\n",(char *)tdata->rsp[td].out.data_iov.sglist[0].iov_base, tdata->rsp[td].out.data_iov.sglist[0].iov_len );
             }
             if (strcmp (mycmd,MK_DIRS) ==0)
             {
                  status = mkdirs(myparm1, atoi(myparm2));
             }
             if (strcmp (mycmd,SET_PERM) ==0)
             {
                  unsigned int premissions;
                  sscanf(myparm2,"%o", &premissions);
                  status =set_permissions(myparm1, premissions);
             }

             if (strcmp (mycmd,SWRITE) == 0 )
             { 
                  char * myfd;
                  char *header_offset;
                  sscanf(myparm1,"%p", &myfd);
                  printf ("the file handle is %p toatal lenght:%ld \n",myfd, sglist[0].iov_len);
                  header_offset=strchr((char *)sglist[0].iov_base, '\n'); 
                  printf ("header_offset: %p  char[-1]=%c char[0]=%c, char[1]=%c i :%d :%d :%d\n",
                        header_offset,  *(header_offset -1), *header_offset,*(header_offset+1),(int)*(header_offset -1),(int)*(header_offset),(int)*(header_offset+1)); 
                  printf ("start_header:%p  header_offset: %p  diff:%p\n",(char *)sglist[0].iov_base, (char *)header_offset, (char *) ((char *)header_offset - (char *)sglist[0].iov_base));
                  status = write_file((FILE *)myfd, atoi(myparm2),atoi(myparm3),header_offset + 1, sglist[0].iov_len - ((header_offset - (char *)sglist[0].iov_base)+1) );

                  sprintf (response_extension,"%s ","response=writedone");   
             }
             if (strcmp (mycmd, SFLUSH) ==0)
             {
                  FILE * myfd; 
                  sscanf(myparm1,"%p", &myfd);               
                  status = flush_file(myfd);
                  sprintf (response_extension,"%s ","response=fflushdone");
             }
             if (strcmp (mycmd,SREAD) ==0 )
             {
                  char * myfd;
                  sscanf(myparm1,"%p", &myfd);               
                  status = read_file((FILE *)myfd,atoi(myparm2),atoi(myparm3),
                          (char *)tdata->rsp[td].out.data_iov.sglist[0].iov_base, &tdata->rsp[td].out.data_iov.sglist[0].iov_len );
                  printf("read_data len:%ld data:%s\n", tdata->rsp[td].out.data_iov.sglist[0].iov_len, (char *)tdata->rsp[td].out.data_iov.sglist[0].iov_base);
             }


        }   
         if ( tdata->rsp[td].out.data_iov.sglist[0].iov_len == 0) {  //nothing from ops above then do default status return response 
            sprintf ((char*)(tdata->rsp[td].out.data_iov.sglist[0].iov_base), "%s%ld %s \n","status=",status, response_extension);
            tdata->rsp[td].out.data_iov.sglist[0].iov_len =
                          strlen((char *)tdata->rsp[td].out.data_iov.sglist[0].iov_base);
         } 
           printf(" exiting rsp len %ld =>%s  is td:[%d]\n", tdata->rsp[td].out.data_iov.sglist[0].iov_len ,(char *)tdata->rsp[td].out.data_iov.sglist[0].iov_base, td);
        req->in.header.iov_base   = NULL;
        req->in.header.iov_len    = 0;
        vmsg_sglist_set_nents(&req->in, 0);
        sglist[0].iov_len=0; 
        sglist[0].iov_base=NULL;
}




/*---------------------------------------------------------------------------*/
/* process_request							     */
/*---------------------------------------------------------------------------*/
static void process_request(struct thread_data *tdata,
			    struct xio_msg *req)
{
/*	if (++tdata->cnt == PRINT_COUNTER) { */
	{ 
		printf("thread [%d] tid:%p - message: [%lu] - %s\n",
		       tdata->affinity,
		       (void *)pthread_self(),
		       (req->sn + 1), (char *)req->in.header.iov_base);
		tdata->cnt = 0;
	}
	req->in.header.iov_base	  = NULL;
	req->in.header.iov_len	  = 0;
	vmsg_sglist_set_nents(&req->in, 0);
}

/*---------------------------------------------------------------------------*/
/* on_request callback							     */
/*---------------------------------------------------------------------------*/
static int on_request(struct xio_session *session,
		      struct xio_msg *req,
		      int last_in_rxq,
		      void *cb_user_context)
{
	struct thread_data	*tdata = (struct thread_data *)cb_user_context;
	int i = req->sn % QUEUE_DEPTH;
        i=0;  //bob temp
	/* process request */
        printf("request session: %p; last_in_rxq:%d\n", session, last_in_rxq);
	process_3par_request(tdata, req);

       if (0)
           process_request(tdata, req);  /* comment out for now  */

	/* attach request to response */
	tdata->rsp[i].request = req; 
        
        tdata->rsp[i].out.sgl_type =XIO_SGL_TYPE_IOV;
        tdata->rsp[i].out.data_iov.nents=1;
        
	xio_send_response(&tdata->rsp[i]);
printf("after onRequest  rsp sent %ld =>%s  is it[%d] \n", tdata->rsp[i].out.data_iov.sglist[0].iov_len ,(char *)tdata->rsp[i].out.data_iov.sglist[0].iov_base, i);
	tdata->nsent++;
        sleep (1); 
	if (test_disconnect) {
                printf("test_disconnect  exit\n");
		if (tdata->nsent == DISCONNECT_NR && tdata->connection) {
			xio_disconnect(tdata->connection);
			return 0;
		}
	}
        //sleep (1);   tmpbob
	return 0;
}

/*---------------------------------------------------------------------------*/
/* asynchronous callbacks						     */
/*---------------------------------------------------------------------------*/
static struct xio_session_ops  portal_server_ops = {
	.on_session_event		=  NULL,
	.on_new_session			=  NULL,
	.on_msg_send_complete		=  NULL,
	.on_msg				=  on_request,
	.on_msg_error			=  NULL
};

/*---------------------------------------------------------------------------*/
/* worker thread callback						     */
/*---------------------------------------------------------------------------*/
static void *portal_server_cb(void *data)
{
	struct thread_data	*tdata = (struct thread_data *)data;
	cpu_set_t		cpuset;
	struct xio_server	*server;
	//char			str[128];
	int			i;

	/* set affinity to thread */

	CPU_ZERO(&cpuset);
	CPU_SET(tdata->affinity, &cpuset);

	pthread_setaffinity_np(tdata->thread_id, sizeof(cpu_set_t), &cpuset);


	/* create thread context for the client */
	tdata->ctx = xio_context_create(NULL, 0, tdata->affinity);

	/* bind a listener server to a portal/url */
	printf("thread [%d] - listen:%s\n", tdata->affinity, tdata->portal);
	server = xio_bind(tdata->ctx, &portal_server_ops, tdata->portal,
			  NULL, 0, tdata);
	if (server == NULL)
		goto cleanup;

//	sprintf(str, "hello world header response from thread %d",
//		tdata->affinity);
#if 0      
	/* create "hell  world" message */
	for (i = 0; i < QUEUE_DEPTH; i++) {
		tdata->rsp[i].out.header.iov_base = strdup(str);
		tdata->rsp[i].out.header.iov_len =
			strlen((const char *)
				tdata->rsp[i].out.header.iov_base) + 1;
               tdata->rsp[i].out.data_iov.sglist[0].iov_base=tdata->rsp[i].out.header.iov_base;
               tdata->rsp[i].out.data_iov.sglist[0].iov_len=tdata->rsp[i].out.header.iov_len;
	}
#endif  
        /* create Tachyon  message response pool */
        for (i = 0; i < QUEUE_DEPTH; i++) {
               tdata->rsp[i].out.data_iov.sglist[0].iov_base= (char *)calloc(1, 1024*1024);
               tdata->rsp[i].out.data_iov.sglist[0].iov_len= 0; //initialize to zero
        }

	/* the default xio supplied main loop */
	xio_context_run_loop(tdata->ctx, XIO_INFINITE);

	/* normal exit phase */
	fprintf(stdout, "****************************** exit signaled\n");

	/* detach the server */
	xio_unbind(server);

	/* free the message */
	for (i = 0; i < QUEUE_DEPTH; i++) {
		free(tdata->rsp[i].out.header.iov_base);
                free(tdata->rsp[i].out.data_iov.sglist[0].iov_base);
        }
cleanup:
	/* free the context */
	xio_context_destroy(tdata->ctx);

	return NULL;
}

static int on_msg_error  (struct xio_session *session,
                            enum xio_status error,
                            enum xio_msg_direction md,
                            struct xio_msg  *msg,
                            void *conn_user_context)
{
     printf("error on msg  session:%p xio_status:%d xio_msg_direction:%d xio_msg:%p conn:%p \n", 
        session, error, md, msg, conn_user_context);
     return 0;
}


/*---------------------------------------------------------------------------*/
/* on_session_event							     */
/*---------------------------------------------------------------------------*/
static int on_session_event(struct xio_session *session,
			    struct xio_session_event_data *event_data,
			    void *cb_user_context)
{
	struct server_data *server_data = (struct server_data *)cb_user_context;
	struct thread_data *tdata;
	//int		   i = 0;


	tdata = (event_data->conn_user_context == server_data) ?
		NULL : (struct thread_data *)event_data->conn_user_context;

	printf("`session event: %s. session:%p, connection:%p, reason: %s\n",
	       xio_session_event_str(event_data->event),
	       session, event_data->conn,
	       xio_strerror(event_data->reason));
        printf ("tdata:%p ", tdata);
        if (tdata)
          printf("portal:%s, affinity:%d, ctx:%p, connection:%p, thread_id:%p\n",
                  tdata->portal, tdata->affinity, tdata->ctx, tdata->connection, (void *)tdata->thread_id);


	switch (event_data->event) {
	case XIO_SESSION_NEW_CONNECTION_EVENT:
		if (tdata)
			tdata->connection = event_data->conn;
                printf("new connection %p \n", event_data->conn);
		break;
	case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
		xio_connection_destroy(event_data->conn);
		if (tdata)
			tdata->connection = NULL;
                printf("connection teardown\n");
		break;
	case XIO_SESSION_TEARDOWN_EVENT:
		xio_session_destroy(session);
//		for (i = 0; i < MAX_THREADS; i++)
//			xio_context_stop_loop(server_data->tdata[i].ctx);
//		xio_context_stop_loop(server_data->ctx);
		break;
	default:
                printf("default case in session event\n");
		break;
	};

	return 0;
}

/*---------------------------------------------------------------------------*/
/* on_new_session							     */
/*---------------------------------------------------------------------------*/
static int on_new_session(struct xio_session *session,
			  struct xio_new_session_req *req,
			  void *cb_user_context)
{
        static int i=0;
	struct portals_vec *portals;
	struct server_data *server_data = (struct server_data *)cb_user_context;

	portals = portals_get(server_data, req->uri, req->private_data);
        
        printf("on new session uri:%s session:%p portal:%s\n", req->uri, session, (char *)portals->vec[0]);
	/* automatic accept the request */
	//xio_accept(session, portals->vec, portals->vec_len, NULL, 0);
	xio_accept(session, &portals->vec[i],1, NULL, 0);
        i = (i + 1) %  MAX_THREADS; 
	portals_free(portals);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* asynchronous callbacks						     */
/*---------------------------------------------------------------------------*/
static struct xio_session_ops  server_ops = {
	.on_session_event		=  on_session_event,
	.on_new_session			=  on_new_session,
	.on_msg_send_complete		=  NULL,
	.on_msg				=  NULL,
	.on_msg_error			=  on_msg_error    //bobf
};

/*---------------------------------------------------------------------------*/
/* main									     */
/*---------------------------------------------------------------------------*/
int main(int argc, char *argv[])
{
	struct xio_server	*server;	/* server portal */
	struct server_data	*server_data;
	char			url[256];
	int			i;
	uint16_t		port = atoi(argv[2]);

	if (argc < 3) {
		printf("Usage: %s <host> <port> <transport:optional>\
				<finite run:optional>\n", argv[0]);
		exit(1);
	}

	server_data = (struct server_data *)calloc(1, sizeof(*server_data));
	if (!server_data)
		return -1;

	xio_init();

	/* create thread context for the client */
	server_data->ctx	= xio_context_create(NULL, 0, -1);

	/* create url to connect to */
	if (argc > 3)
		sprintf(url, "%s://%s:%d", argv[3], argv[1], port);
	else
		sprintf(url, "rdma://%s:%d", argv[1], port);

	if (argc > 4)
		test_disconnect = atoi(argv[4]);
	else
		test_disconnect = 1;

	/* bind a listener server to a portal/url */
	server = xio_bind(server_data->ctx, &server_ops,
			  url, NULL, 0, server_data);
	if (server == NULL)
		goto cleanup;


	/* spawn portals */
	for (i = 0; i < MAX_THREADS; i++) {
		server_data->tdata[i].affinity = i+1;
		port += 1;
		if (argc > 3)
			sprintf(server_data->tdata[i].portal, "%s://%s:%d",
				argv[3], argv[1], port);
		else
			sprintf(server_data->tdata[i].portal, "rdma://%s:%d",
				argv[1], port);
		pthread_create(&server_data->tdata[i].thread_id, NULL,
			       portal_server_cb, &server_data->tdata[i]);
	}

	xio_context_run_loop(server_data->ctx, XIO_INFINITE);

	/* normal exit phase */
	fprintf(stdout, "exit signaled\n");

	/* join the threads */
	for (i = 0; i < MAX_THREADS; i++)
		pthread_join(server_data->tdata[i].thread_id, NULL);

	/* free the server */
	xio_unbind(server);
cleanup:
	/* free the context */
	xio_context_destroy(server_data->ctx);

	xio_shutdown();

	free(server_data);

	return 0;
}

