import socket
import sys
import json
import heapq
import atexit
from socketserver import BaseRequestHandler, ThreadingTCPServer
from json import JSONDecodeError
from collections import defaultdict
from threading import Lock

def print_useage():
    print("ProtoHackers Job Centre server.")
    print("Useage:")
    print("python job_centre.py <<PORT>>")
    print("i.e. python job_centre.py 5900")
    exit()

class Job:

    job_id_lock = Lock() # ensure atomic incrementing of next job id
    job_id_counter = -1 # next unique job id
    job_obj_by_id = dict()

    def get_job_id():
        with Job.job_id_lock:
            Job.job_id_counter += 1
            return Job.job_id_counter

    def __init__(self, json_msg):
        self.job = json_msg["job"]
        self.job_id = Job.get_job_id()
        self.pri  = json_msg["pri"]
        self.queue = json_msg["queue"]
        Job.job_obj_by_id[self.job_id] = self

    def __lt__(self, other):
        return self.pri > other.pri

    def __repr__(self):
        return "Job ID:" + str(self.job_id) + " with pri " + str(self.pri)


class JobCentreServer(BaseRequestHandler):

    sessions = set()
    queue_mgmt_lock = Lock() # ensure atomic operations on job queues
    named_queues = dict() # dict of heapq
    job_ids_in_queues = set() # all jobs that exist, but not being worked
    client_running_job_id = dict() # k-v pair of job-id : Client
    waiting_threads = defaultdict(set) # clients awaiting a job assignment to one of the id'd queues

    def get_next_msg(self):
        buf = b''
        while True:
            part = self.request.recv(1024)
            if not part:
                raise RuntimeError("socket connection broken")
            buf += part
            while b'\n' in buf:
                msg = buf[0:buf.index(b'\n')+1]
                buf = buf[buf.index(b'\n')+1:]
                yield msg

    def send_error(self):
        err = {"status": "error"}
        err = json.dumps(err)
        err = err.encode()
        self.request.send(err)

    def send_nojob(self):
        err = {"status": "no-job"}
        err = json.dumps(err)
        err = err.encode()
        self.request.send(err)

    def send_ok(self):
        msg = {"status": "ok"}
        msg = json.dumps(msg)
        msg = msg.encode()
        self.request.send(msg)

    def validate(self, msg):
        json_msg = json.loads(msg) # Raises JSONDecodeError if incorrect
        if "request" not in json_msg:
            raise RuntimeError("no request field")
        if json_msg["request"] not in ["put", "get", "delete", "abort"]:
            raise RuntimeError("invalid request field: " + str(json_msg["status"]))
        if json_msg["request"] == "put":
            if any(x not in json_msg for x in ["queue", "job", "pri"]):
                raise RuntimeError("required field missing")
            if len(json_msg["queue"]) == 0:
                raise RuntimeError("invalid queue name")
            if not isinstance(json_msg["pri"], int) or json_msg["pri"] < 0:
                raise RuntimeError("invalid job priority")
            return json_msg
        if json_msg["request"] == "get":
            if any(x not in json_msg for x in ["queues"]):
                raise RuntimeError("required field missing")
            if len(json_msg["queues"]) == 0:
                raise RuntimeError("invalid queues list")
            return json_msg
        if json_msg["request"] == "delete":
            if any(x not in json_msg for x in ["id"]):
                raise RuntimeError("required field missing")
            if not isinstance(json_msg["id"], int) or json_msg["id"] < 0:
                raise RuntimeError("invalid id field")
            return json_msg
        if json_msg["request"] == "abort":
            if any(x not in json_msg for x in ["id"]):
                raise RuntimeError("required field missing")
            if not isinstance(json_msg["id"], int) or json_msg["id"] < 0:
                raise RuntimeError("invalid id field")
            return json_msg

    def push_assign(self, queue_name):
        with JobCentreServer.queue_mgmt_lock:
            if len(JobCentreServer.named_queues[queue_name]) == 0:
                return
            selected_job = heapq.heappop(JobCentreServer.named_queues[queue_name])
            JobCentreServer.job_ids_in_queues.remove(selected_job.job_id)
            del JobCentreServer.waiting_threads[self]
            self.this_clients_running_jobs[selected_job.job_id] = selected_job
            JobCentreServer.client_running_job_id[selected_job.job_id] = self
            response = {"status": "ok"}
            response["id"] = selected_job.job_id
            response["pri"] = selected_job.pri
            response["queue"] = selected_job.queue
            response["job"] = selected_job.job
            response = json.dumps(response)
            response = response.encode()
            self.request.send(response)

    def push_delete(self, t_id):
        del JobCentreServer.client_running_job_id[t_id]
        del self.this_clients_running_jobs[t_id]

    def handle_msg(self, json_msg):
        if json_msg["request"] == "put":
            new_job = Job(json_msg)
            with JobCentreServer.queue_mgmt_lock:
                if json_msg["queue"] not in JobCentreServer.named_queues:
                    JobCentreServer.named_queues[json_msg["queue"]] = []
                heapq.heappush(JobCentreServer.named_queues[json_msg["queue"]], new_job)
            JobCentreServer.job_ids_in_queues.add(new_job.job_id)
            response = {"status": "ok"}
            response["id"] = new_job.job_id
            response = json.dumps(response)
            response = response.encode()
            self.request.send(response)

            # We check and see if any client was waiting for a 
            # job to be assigned to the selected queue
            for client, queue_set in JobCentreServer.waiting_threads.items():
                if json_msg["queue"] in queue_set:
                    client.push_assign(json_msg["queue"])
                    return

        if json_msg["request"] == "get":
            highest_pri = -1
            highest_pri_q = None
            with JobCentreServer.queue_mgmt_lock:
                for q in json_msg["queues"]:
                    if q in JobCentreServer.named_queues and len(JobCentreServer.named_queues[q]) > 0:
                        if JobCentreServer.named_queues[q][0].pri > highest_pri:
                            highest_pri = JobCentreServer.named_queues[q][0].pri
                            highest_pri_q = q
                if highest_pri_q == None:
                    if "wait" in json_msg and json_msg["wait"]:
                        JobCentreServer.waiting_threads[self] = set(json_msg["queues"])
                        return
                    else:
                        self.send_nojob()
                        return
                selected_job = heapq.heappop(JobCentreServer.named_queues[highest_pri_q])
                JobCentreServer.job_ids_in_queues.remove(selected_job.job_id)
                JobCentreServer.client_running_job_id[selected_job.job_id] = self
                self.this_clients_running_jobs[selected_job.job_id] = selected_job
            response = {"status": "ok"}
            response["id"] = selected_job.job_id
            response["pri"] = selected_job.pri
            response["queue"] = highest_pri_q
            response["job"] = selected_job.job
            response = json.dumps(response)
            response = response.encode()
            self.request.send(response)

        if json_msg["request"] == "delete":
            t_id = json_msg["id"]
            if t_id not in JobCentreServer.client_running_job_id and t_id not in JobCentreServer.job_ids_in_queues:
                self.send_nojob()
                return
            if t_id > Job.job_id_counter:
                self.send_nojob()
                return
            if t_id in JobCentreServer.client_running_job_id:
                JobCentreServer.client_running_job_id[t_id].push_delete(t_id)
                self.send_ok()
                return
            if t_id in JobCentreServer.job_ids_in_queues:
                with JobCentreServer.queue_mgmt_lock:
                    JobCentreServer.job_ids_in_queues.remove(t_id)
                    t_q = Job.job_obj_by_id[t_id].queue
                    JobCentreServer.named_queues[t_q].remove(Job.job_obj_by_id[t_id])
                    heapq.heapify(JobCentreServer.named_queues[t_q])
                self.send_ok()

        if json_msg["request"] == "abort":
            t_id = json_msg["id"]
            if t_id not in self.this_clients_running_jobs:
                self.send_nojob()
                return
            aborted_job = self.this_clients_running_jobs[t_id]
            del self.this_clients_running_jobs[t_id]
            if t_id in JobCentreServer.client_running_job_id:
                del JobCentreServer.client_running_job_id[t_id]
            with JobCentreServer.queue_mgmt_lock:
                heapq.heappush(JobCentreServer.named_queues[aborted_job.queue], aborted_job)
                JobCentreServer.job_ids_in_queues.add(t_id)
            self.send_ok()


    def handle(self):
        JobCentreServer.sessions.add(self)
        self.this_clients_running_jobs = dict()
        try:
            while True:
                # wait for message from client
                for msg in self.get_next_msg():
                    if not msg:  # client closed socket
                        raise ConnectionError("No message or disconnect")
                    try:
                        json_msg = self.validate(msg)
                    except JSONDecodeError:
                        self.send_error()
                    except RuntimeError as e:
                        self.send_error()
                        print(e)
                    # json_msg contains message from the client
                    # it is valid JSON and contains the required fields
                    print("Got msg", json_msg)
                    self.handle_msg(json_msg)

        except ConnectionError: # client terminated abruptly
            pass
        except RuntimeError: # client terminated abruptly
            pass
        finally:
            # the client closed the connection, so need to abort any jobs they were assigned
            # and place them back in queue
            if self in JobCentreServer.waiting_threads:
                del JobCentreServer.waiting_threads[self]
            seen_queues = set()
            with JobCentreServer.queue_mgmt_lock:
                for aborted_job_id, aborted_job in self.this_clients_running_jobs.items():
                    if aborted_job_id in JobCentreServer.client_running_job_id:
                        del JobCentreServer.client_running_job_id[aborted_job_id]
                    seen_queues.add(aborted_job.queue)
                    heapq.heappush(JobCentreServer.named_queues[aborted_job.queue], aborted_job)
                    JobCentreServer.job_ids_in_queues.add(aborted_job_id)
                # We check and see if any client was waiting for a 
                # job to be assigned to the selected queue
            for client, queue_set in JobCentreServer.waiting_threads.items():
                for q in seen_queues:
                    if q in queue_set:
                        client.push_assign(q)

            if self.request:
                self.request.close()
            JobCentreServer.sessions.remove(self)

    @classmethod
    def shutdown(cls):
        for session in JobCentreServer.sessions:
            if session.request:
                session.request.close()

@atexit.register
def shutdown_chat_server():
    print('Closing job client sockets, ', end='')
    JobCentreServer.shutdown()
    print('shutting down server...')
    server.shutdown()

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_useage()

    port = int(sys.argv[1])

    try:
        # Allow server to rebind to previously used port number.
        ThreadingTCPServer.allow_reuse_address = True
        server = ThreadingTCPServer(('', port), JobCentreServer)
        print(f'Starting JobCentreServer at port {port}...')
        server.serve_forever()
    except KeyboardInterrupt:  # catch Ctrl-c
        pass  # Interpreter will call atexit handler

