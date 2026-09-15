use std::collections::HashMap;
use std::env;
use std::io::{Read,Write};
use std::net::{TcpListener,TcpStream,ToSocketAddrs};
use std::sync::{Arc,Mutex};
use std::thread;
use std::time::{Duration,SystemTime,UNIX_EPOCH};

const HEARTBEAT_TIMEOUT_SECS:u64=5;
const SCHEDULER_INTERVAL_MS:u64=250;
const CONNECT_TIMEOUT_SECS:u64=3;
const HTTP_TIMEOUT_SECS:u64=35;

#[derive(Clone,Debug)]
struct Task {
    id:u64,
    kind:String,
    value:String,
    status:String,
    result:String,
    assigned_worker:Option<String>,
    attempts:u32,
}

#[derive(Clone,Debug)]
struct Worker {
    id:String,
    address:String,
    last_seen:u64,
    busy:bool,
}

struct CoordinatorState {
    next_task_id:u64,
    tasks:HashMap<u64,Task>,
    workers:HashMap<String,Worker>,
}

fn now_secs()->u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn parse_kv(body:&str)->HashMap<String,String> {
    body.split('&').filter_map(|part|{
        let mut pieces=part.splitn(2,'=');
        let key=pieces.next()?.trim();
        let value=pieces.next()?.trim();
        if key.is_empty(){None}else{Some((key.to_string(),value.to_string()))}
    }).collect()
}

fn http_response(status:&str,body:&str)->String {
    format!("HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())
}

fn send_response(mut stream:TcpStream,status:&str,body:&str) {
    let _=stream.write_all(http_response(status,body).as_bytes());
}

fn read_request(stream:&mut TcpStream)->Option<(String,String,String)> {
    let mut buffer=Vec::new();
    let mut temp=[0u8;4096];
    loop {
        let count=stream.read(&mut temp).ok()?;
        if count==0{break;}
        buffer.extend_from_slice(&temp[..count]);
        if buffer.windows(4).any(|window|window==b"\r\n\r\n"){break;}
        if buffer.len()>64*1024{return None;}
    }
    let header_end=buffer.windows(4).position(|window|window==b"\r\n\r\n")?;
    let headers=String::from_utf8_lossy(&buffer[..header_end]);
    let first_line=headers.lines().next()?;
    let mut parts=first_line.split_whitespace();
    let method=parts.next()?.to_string();
    let path=parts.next()?.to_string();
    let mut content_length=0usize;
    for line in headers.lines().skip(1) {
        if let Some(value)=line.strip_prefix("Content-Length:"){content_length=value.trim().parse().unwrap_or(0);}
    }
    let body_start=header_end+4;
    let mut body=buffer[body_start..].to_vec();
    while body.len()<content_length {
        let count=stream.read(&mut temp).ok()?;
        if count==0{break;}
        body.extend_from_slice(&temp[..count]);
    }
    body.truncate(content_length);
    Some((method,path,String::from_utf8_lossy(&body).into_owned()))
}

fn http_request(address:&str,method:&str,path:&str,body:&str)->Result<String,String> {
    let socket_address=address.to_socket_addrs().map_err(|error|format!("failed to resolve {address}: {error}"))?.next().ok_or_else(||format!("could not resolve address: {address}"))?;
    let mut stream=TcpStream::connect_timeout(&socket_address,Duration::from_secs(CONNECT_TIMEOUT_SECS)).map_err(|error|format!("failed to connect to {address}: {error}"))?;
    let request=format!(" {method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
    let request=request.trim_start().to_string();
    stream.write_all(request.as_bytes()).map_err(|error|error.to_string())?;
    let mut response=Vec::new();
    stream.read_to_end(&mut response).map_err(|error|error.to_string())?;
    let response=String::from_utf8_lossy(&response);
    let body=response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    if response.starts_with("HTTP/1.1 200"){Ok(body)}else{Err(response.lines().next().unwrap_or("HTTP error").to_string())}
}

fn execute_task(kind:&str,value:&str)->Result<String,String> {
    match kind {
        "echo"=>Ok(value.to_string()),
        "add"=>{
            let numbers:Vec<i64>=value.split(',').map(|item|item.trim().parse::<i64>().map_err(|_|format!("invalid number: {}",item.trim()))).collect::<Result<_,_>>()?;
            if numbers.len()!=2{return Err("add requires exactly two numbers".to_string());}
            Ok((numbers[0]+numbers[1]).to_string())
        },
        "fib"=>{
            let n:u64=value.parse().map_err(|_|"fib requires a non-negative integer".to_string())?;
            if n>45{return Err("fib input must be <= 45".to_string());}
            Ok(fibonacci(n).to_string())
        },
        "sleep"=>{
            let seconds:u64=value.parse().map_err(|_|"sleep requires a number of seconds".to_string())?;
            if seconds>30{return Err("sleep duration must be <= 30 seconds".to_string());}
            thread::sleep(Duration::from_secs(seconds));
            Ok(format!("slept for {seconds} seconds"))
        },
        _=>Err(format!("unknown task type: {kind}")),
    }
}

fn fibonacci(n:u64)->u64 {
    match n {
        0=>0,
        1=>1,
        _=>fibonacci(n-1)+fibonacci(n-2),
    }
}

fn coordinator_handler(mut stream:TcpStream,state:Arc<Mutex<CoordinatorState>>) {
    let Some((method,path,body))=read_request(&mut stream) else{return;};

    if method=="GET"&&path=="/health" {
        send_response(stream,"200 OK","coordinator: healthy\n");
        return;
    }

    if method=="POST"&&path=="/register" {
        let values=parse_kv(&body);
        let Some(id)=values.get("id") else {
            send_response(stream,"400 Bad Request","missing id\n");
            return;
        };
        let Some(address)=values.get("address") else {
            send_response(stream,"400 Bad Request","missing address\n");
            return;
        };
        let mut state=state.lock().unwrap();
        let existing_busy=state.workers.get(id).map(|worker|worker.busy).unwrap_or(false);
        state.workers.insert(id.clone(),Worker{id:id.clone(),address:address.clone(),last_seen:now_secs(),busy:existing_busy});
        println!("Worker {} registered at {}",id,address);
        send_response(stream,"200 OK","registered\n");
        return;
    }

    if method=="POST"&&path=="/heartbeat" {
        let values=parse_kv(&body);
        let Some(id)=values.get("id") else {
            send_response(stream,"400 Bad Request","missing id\n");
            return;
        };
        let mut state=state.lock().unwrap();
        if let Some(worker)=state.workers.get_mut(id) {
            worker.last_seen=now_secs();
            send_response(stream,"200 OK","heartbeat accepted\n");
        } else {
            send_response(stream,"404 Not Found","worker not registered\n");
        }
        return;
    }

    if method=="GET"&&path=="/workers" {
        let state=state.lock().unwrap();
        let now=now_secs();
        let mut output=String::new();
        for worker in state.workers.values() {
            let age=now.saturating_sub(worker.last_seen);
            let status=if age<=HEARTBEAT_TIMEOUT_SECS {
                if worker.busy{"busy"}else{"idle"}
            }else{"dead"};
            output.push_str(&format!("{} {} {} last_seen={}s_ago\n",worker.id,worker.address,status,age));
        }
        if output.is_empty(){output.push_str("no workers registered\n");}
        send_response(stream,"200 OK",&output);
        return;
    }

    if method=="POST"&&path=="/tasks" {
        let values=parse_kv(&body);
        let Some(kind)=values.get("type") else {
            send_response(stream,"400 Bad Request","missing type\n");
            return;
        };
        let Some(value)=values.get("value") else {
            send_response(stream,"400 Bad Request","missing value\n");
            return;
        };
        if !matches!(kind.as_str(),"echo"|"add"|"fib"|"sleep") {
            send_response(stream,"400 Bad Request","unsupported task type\n");
            return;
        }
        let mut state=state.lock().unwrap();
        let id=state.next_task_id;
        state.next_task_id+=1;
        state.tasks.insert(id,Task{id,kind:kind.clone(),value:value.clone(),status:"queued".to_string(),result:String::new(),assigned_worker:None,attempts:0});
        println!("Task {} queued: type={}, value={}",id,kind,value);
        send_response(stream,"200 OK",&format!("task_id={id}\nstatus=queued\n"));
        return;
    }

    if method=="GET"&&path.starts_with("/tasks/") {
        let id_text=path.trim_start_matches("/tasks/");
        let Ok(id)=id_text.parse::<u64>() else {
            send_response(stream,"400 Bad Request","invalid task id\n");
            return;
        };
        let state=state.lock().unwrap();
        if let Some(task)=state.tasks.get(&id) {
            let assigned=task.assigned_worker.as_deref().unwrap_or("none");
            let body=format!("task_id={}\ntype={}\nvalue={}\nstatus={}\nresult={}\nworker={}\nattempts={}\n",task.id,task.kind,task.value,task.status,task.result,assigned,task.attempts);
            send_response(stream,"200 OK",&body);
        } else {
            send_response(stream,"404 Not Found","task not found\n");
        }
        return;
    }

    send_response(stream,"404 Not Found","not found\n");
}

fn run_coordinator(port:u16) {
    let listener=TcpListener::bind(("0.0.0.0",port)).expect("failed to bind coordinator port");
    let state=Arc::new(Mutex::new(CoordinatorState{next_task_id:1,tasks:HashMap::new(),workers:HashMap::new()}));
    let scheduler_state=Arc::clone(&state);
    thread::spawn(move||scheduler_loop(scheduler_state));
    println!("Distributed task scheduler coordinator listening on port {port}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream)=>{
                let state=Arc::clone(&state);
                thread::spawn(move||coordinator_handler(stream,state));
            },
            Err(error)=>eprintln!("coordinator connection error: {error}"),
        }
    }
}

fn scheduler_loop(state:Arc<Mutex<CoordinatorState>>) {
    loop {
        thread::sleep(Duration::from_millis(SCHEDULER_INTERVAL_MS));
        let now=now_secs();

        let assignments={
            let mut state=state.lock().unwrap();

            let dead_workers:Vec<String>=state.workers.iter().filter_map(|(id,worker)|{
                if now.saturating_sub(worker.last_seen)>HEARTBEAT_TIMEOUT_SECS{Some(id.clone())}else{None}
            }).collect();

            for worker_id in dead_workers {
                eprintln!("Worker {} missed heartbeat",worker_id);
                if let Some(worker)=state.workers.remove(&worker_id) {
                    if worker.busy {
                        for task in state.tasks.values_mut() {
                            if task.assigned_worker.as_deref()==Some(worker_id.as_str())&&task.status=="running" {
                                task.status="queued".to_string();
                                task.assigned_worker=None;
                                eprintln!("Requeued task {} after worker failure",task.id);
                            }
                        }
                    }
                }
            }

            let available_workers:Vec<Worker>=state.workers.values().filter(|worker|{
                !worker.busy&&now.saturating_sub(worker.last_seen)<=HEARTBEAT_TIMEOUT_SECS
            }).cloned().collect();

            let mut assignments=Vec::new();

            for worker in available_workers {
                let task_id=state.tasks.values().find(|task|task.status=="queued").map(|task|task.id);
                let Some(task_id)=task_id else{break;};

                let assignment=if let Some(task)=state.tasks.get_mut(&task_id) {
                    task.status="running".to_string();
                    task.assigned_worker=Some(worker.id.clone());
                    task.attempts+=1;
                    Some((worker.id.clone(),worker.address.clone(),task.kind.clone(),task.value.clone(),task.id))
                } else {None};

                if let Some(assignment)=assignment {
                    if let Some(worker_state)=state.workers.get_mut(&worker.id){worker_state.busy=true;}
                    assignments.push(assignment);
                }
            }

            assignments
        };

        for(worker_id,address,kind,value,task_id) in assignments {
            let state=Arc::clone(&state);
            thread::spawn(move||{
                println!("Assigning task {} to {}",task_id,worker_id);
                let body=format!("task_id={}&type={}&value={}",task_id,kind,value);
                let result=http_request(&address,"POST","/execute",&body);
                let mut state=state.lock().unwrap();

                if let Some(worker)=state.workers.get_mut(&worker_id) {
                    worker.busy=true;
                }

                if let Some(task)=state.tasks.get_mut(&task_id) {
                    match result {
                        Ok(response)=>{
                            if response.starts_with("status=done") {
                                let result_value=response.lines().find_map(|line|line.strip_prefix("result=")).unwrap_or("").to_string();
                                task.status="completed".to_string();
                                task.result=result_value;
                                task.assigned_worker=None;
                                if let Some(worker)=state.workers.get_mut(&worker_id){worker.busy=false;}
                                println!("Task {} completed by {}",task_id,worker_id);
                            } else if response.starts_with("status=failed") {
                                let result_value=response.lines().find_map(|line|line.strip_prefix("result=")).unwrap_or("task failed").to_string();
                                task.status="failed".to_string();
                                task.result=result_value;
                                task.assigned_worker=None;
                                if let Some(worker)=state.workers.get_mut(&worker_id){worker.busy=false;}
                                println!("Task {} failed on {}",task_id,worker_id);
                            }
                        },
                        Err(error)=>{
                            eprintln!("Worker {} failed task {}: {}",worker_id,task_id,error);
                            if let Some(worker)=state.workers.get_mut(&worker_id){worker.busy=true;}
                        }
                    }
                }
            });
        }
    }
}

fn worker_handler(mut stream:TcpStream) {
    let Some((method,path,body))=read_request(&mut stream) else{return;};

    if method=="GET"&&path=="/health" {
        send_response(stream,"200 OK","worker: healthy\n");
        return;
    }

    if method=="POST"&&path=="/execute" {
        let values=parse_kv(&body);
        let Some(task_id)=values.get("task_id") else {
            send_response(stream,"400 Bad Request","missing task_id\n");
            return;
        };
        let Some(kind)=values.get("type") else {
            send_response(stream,"400 Bad Request","missing type\n");
            return;
        };
        let Some(value)=values.get("value") else {
            send_response(stream,"400 Bad Request","missing value\n");
            return;
        };

        println!("Executing task {}: type={}, value={}",task_id,kind,value);

        match execute_task(kind,value) {
            Ok(result)=>send_response(stream,"200 OK",&format!("status=done\ntask_id={task_id}\nresult={result}\n")),
            Err(error)=>send_response(stream,"200 OK",&format!("status=failed\ntask_id={task_id}\nresult={error}\n")),
        }
        return;
    }

    send_response(stream,"404 Not Found","not found\n");
}

fn run_worker(id:String,port:u16,address:String,coordinator:String) {
    let listener=TcpListener::bind(("0.0.0.0",port)).expect("failed to bind worker port");
    println!("Worker {} listening on {}, coordinator={}",id,address,coordinator);

    let registration_id=id.clone();
    let registration_address=address.clone();
    let registration_coordinator=coordinator.clone();

    thread::spawn(move||{
        loop {
            let body=format!("id={}&address={}",registration_id,registration_address);
            match http_request(&registration_coordinator,"POST","/register",&body) {
                Ok(_)=>{
                    println!("Worker {} registered",registration_id);
                    break;
                },
                Err(error)=>{
                    eprintln!("Worker {} registration failed: {}",registration_id,error);
                    thread::sleep(Duration::from_secs(2));
                }
            }
        }
    });

    let heartbeat_id=id.clone();
    let heartbeat_address=address.clone();
    let heartbeat_coordinator=coordinator.clone();

    thread::spawn(move||{
        loop {
            let body=format!("id={}",heartbeat_id);
            match http_request(&heartbeat_coordinator,"POST","/heartbeat",&body) {
                Ok(_)=>{},
                Err(error)=>{
                    eprintln!("Worker {} heartbeat failed: {}",heartbeat_id,error);
                    let register_body=format!("id={}&address={}",heartbeat_id,heartbeat_address);
                    match http_request(&heartbeat_coordinator,"POST","/register",&register_body) {
                        Ok(_)=>println!("Worker {} re-registered",heartbeat_id),
                        Err(register_error)=>eprintln!("Worker {} re-registration failed: {}",heartbeat_id,register_error),
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
    });

    for stream in listener.incoming() {
        match stream {
            Ok(stream)=>{
                thread::spawn(move||worker_handler(stream));
            }
            Err(error)=>{
                eprintln!("worker connection error: {}",error);
            }
        }
    }
}

fn env_string(name:&str,default:&str)->String {
    env::var(name).unwrap_or_else(|_|default.to_string())
}

fn env_u16(name:&str,default:u16)->u16 {
    env::var(name).ok().and_then(|value|value.parse().ok()).unwrap_or(default)
}

fn main() {
    let role=env_string("SCHEDULER_ROLE","coordinator");

    match role.as_str() {
        "coordinator"=>{
            let port=env_u16("SCHEDULER_PORT",8080);
            run_coordinator(port);
        },
        "worker"=>{
            let id=env_string("WORKER_ID","worker-1");
            let port=env_u16("WORKER_PORT",9001);
            let address=env_string("WORKER_ADDRESS","127.0.0.1:9001");
            let coordinator=env_string("COORDINATOR_ADDRESS","127.0.0.1:8080");
            run_worker(id,port,address,coordinator);
        },
        _=>{
            eprintln!("Invalid SCHEDULER_ROLE '{}'. Use 'coordinator' or 'worker'.",role);
            std::process::exit(1);
        }
    }
}