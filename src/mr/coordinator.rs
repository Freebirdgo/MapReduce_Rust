use std::{sync::{Arc, Mutex}, collections::{HashMap, HashSet}, time::Duration};

use futures::future::{Ready, ready};
use tarpc::context;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct Coordinator {
    map_tasks: Arc<Mutex<HashMap<i32, bool>>>,
    map_id: Arc<Mutex<i32>>,
    reduce_tasks: Arc<Mutex<HashMap<i32, bool>>>,
    reduce_id: Arc<Mutex<i32>>,
    map_n: i32,
    reduce_n: i32,
    worker_n: i32,
    map_finish: Arc<Mutex<bool>>,
    reduce_finish: Arc<Mutex<bool>>,
    worker_id: Arc<Mutex<i32>>,
    map_leases: Arc<Mutex<HashMap<i32, Instant>>>,
    reduce_leases: Arc<Mutex<HashMap<i32, Instant>>>,
   
}

impl Coordinator {
    pub fn new(map_n: i32, reduce_n: i32, worker_n: i32) -> Self {
        Self {
            map_tasks: Arc::new(Mutex::new(HashMap::new())),
            map_id: Arc::new(Mutex::new(0)),
            reduce_tasks: Arc::new(Mutex::new(HashMap::new())),
            reduce_id: Arc::new(Mutex::new(0)),
            map_n,
            reduce_n,
            worker_n,
            map_finish: Arc::new(Mutex::new(false)),
            reduce_finish: Arc::new(Mutex::new(false)),
            worker_id: Arc::new(Mutex::new(0)),
            map_leases: Arc::new(Mutex::new(HashMap::new())),
            reduce_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn prepare(&self) -> bool {
        *self.worker_id.lock().unwrap() == self.worker_n
    }

    pub fn done(&self) -> bool {
        *self.map_finish.lock().unwrap() && *self.reduce_finish.lock().unwrap()
    }

    pub fn check_lease(&mut self) -> bool {
        let _map_id = self.map_id.lock().unwrap();
        let _reduce_id = self.reduce_id.lock().unwrap();
        let _worker_id = self.worker_id.lock().unwrap();
        let mut map_tasks = self.map_tasks.lock().unwrap();
        let mut reduce_tasks = self.reduce_tasks.lock().unwrap();
        let mut map_leases = self.map_leases.lock().unwrap();
        let mut reduce_leases = self.reduce_leases.lock().unwrap();
        let reduce_finish = self.reduce_finish.lock().unwrap();
        let map_finish = self.map_finish.lock().unwrap();

        if *map_finish && *reduce_finish {
            return true;
        }

        if *map_finish {
            println!("[Check Lease] The MapReduce is in reduce phase, begin to check reduce tasks leases");
            assert!(!*reduce_finish);
            let dead_reduce_tasks = reduce_leases
                .iter()
                .filter(|(_, time)| time.elapsed() >= Duration::new(5, 0))
                .map(|x| *x.0)
                .collect::<HashSet<i32>>();
            for dead_id in &dead_reduce_tasks {
                assert!(reduce_tasks.get(dead_id).unwrap());
                println!("[Check Lease] deadd reduce task #{} detected, mark it as deadd", dead_id);
                reduce_tasks.insert(*dead_id, false);
                reduce_leases.remove_entry(dead_id);
            }
            return true;
        }

        assert!(!*map_finish && !*reduce_finish);
        println!("[Check Lease] The MapReduce is in map phase, begin to check map tasks leases");
        let dead_map_tasks = map_leases
            .iter()
            .filter(|(_, time)| time.elapsed() >= Duration::new(5, 0))
            .map(|x| *x.0)
            .collect::<HashSet<i32>>();
        for dead_id in &dead_map_tasks {
            assert!(map_tasks.get(dead_id).unwrap());
            println!("[Check Lease] deadd map task #{} detected, mark it as deadd", dead_id);
            map_tasks.insert(*dead_id, false);
            map_leases.remove_entry(dead_id);
        }

        true
    }

    
}

#[tarpc::service]
pub trait Server {
    async fn get_map_task() -> i32;
    async fn get_reduce_task() -> i32;
    async fn get_worker_id() -> i32;
    async fn report_map_task_finish(id: i32) -> bool;
    async fn report_reduce_task_finish(id: i32) -> bool;
    async fn renew_map_lease(id: i32) -> bool;
    async fn renew_reduce_lease(id: i32) -> bool;
}

#[tarpc::server]
impl Server for Coordinator {
    type GetMapTaskFut = Ready<i32>;
    type GetReduceTaskFut = Ready<i32>;
    type GetWorkerIdFut = Ready<i32>;
    type ReportMapTaskFinishFut = Ready<bool>;
    type ReportReduceTaskFinishFut = Ready<bool>;
    type RenewMapLeaseFut = Ready<bool>;
    type RenewReduceLeaseFut = Ready<bool>;

    fn renew_map_lease(self, _: context::Context, id: i32) -> Self::RenewMapLeaseFut {
        let mut map_lease = self.map_leases.lock().unwrap();
        assert!(map_lease.contains_key(&id));
        map_lease.insert(id, Instant::now());
        ready(true)
    }

    fn renew_reduce_lease(self, _: context::Context, id: i32) -> Self::RenewReduceLeaseFut {
        let mut reduce_lease = self.reduce_leases.lock().unwrap();
        assert!(reduce_lease.contains_key(&id));
        reduce_lease.insert(id, Instant::now());
        ready(true)
    }

    fn get_map_task(self, _: context::Context) -> Self::GetMapTaskFut {
        let mut cur_map_id = self.map_id.lock().unwrap();
        let mut cur_map_tasks = self.map_tasks.lock().unwrap();
        let mut cur_map_leases = self.map_leases.lock().unwrap();

        if !self.prepare() {
            return ready(-2);
        }

        if *cur_map_id == self.map_n || *self.map_finish.lock().unwrap() {
           
            for (&k, &v) in &cur_map_tasks.clone() {
                if v {
                    continue;
                }
                println!("[Map] deadd map task #{} detected, the previous worker may have gone offline, assigned this task to a new worker", k);
                cur_map_tasks.insert(k, true);
                assert!(!cur_map_leases.contains_key(&k));
                cur_map_leases.insert(k, Instant::now());
                return ready(k);
            }
            if !cur_map_leases.is_empty() {
                return ready(-3);
            }
            return ready(-1);
        }

        cur_map_tasks.insert(*cur_map_id, true);
        cur_map_leases.insert(*cur_map_id, Instant::now());
        let cur_map = *cur_map_id;
        let ret = ready(cur_map);
        // Increase the global unique map task id by one
        *cur_map_id += 1;
        println!("[Map] Assigned map task #{} to worker", cur_map);
        if cur_map + 1 == self.map_n {
            println!("[Map] All available map tasks have been assigned to worker, wait til all worker processes finish the map phase");
        }
        // Return the map task id
        ret
    }

    fn get_reduce_task(self, _: context::Context) -> Self::GetReduceTaskFut {
        let mut cur_reduce_id = self.reduce_id.lock().unwrap();
        let mut cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        let mut cur_reduce_leases = self.reduce_leases.lock().unwrap();

        if !*self.map_finish.lock().unwrap() {
            return ready(-2);
        }

        if *cur_reduce_id == self.reduce_n || *self.reduce_finish.lock().unwrap() {
            for (&k, &v) in &cur_reduce_tasks.clone() {
                if v {
                    continue;
                }
                println!("[Reduce] deadd reduce task #{} detected, the previous worker may have gone offline, assigned this task to a new worker", k);
                cur_reduce_tasks.insert(k, true);
                assert!(!cur_reduce_leases.contains_key(&k));
                // Update the lease
                cur_reduce_leases.insert(k, Instant::now());
                return ready(k);
            }
            if !cur_reduce_leases.is_empty() {
                return ready(-3);
            }
            return ready(-1);
        }

        cur_reduce_tasks.insert(*cur_reduce_id, true);
        cur_reduce_leases.insert(*cur_reduce_id, Instant::now());
        let cur_reduce = *cur_reduce_id;
        let ret = ready(cur_reduce);
        *cur_reduce_id += 1;
        println!("[Reduce] Assigned reduce task #{} to worker", cur_reduce);
        if cur_reduce + 1 == self.reduce_n {
            println!("[Reduce] All available reduce tasks have been assigned to worker, wait til all worker processes finish the reduce phase");
        }
        ret
    }

    fn get_worker_id(self, _: context::Context) -> Self::GetWorkerIdFut {
        let mut cur_worker_id = self.worker_id.lock().unwrap();
        let cur_num = *cur_worker_id;
        assert!(cur_num != self.worker_n);
        let left_num = self.worker_n - cur_num - 1;
        let ret = ready(cur_num);
        *cur_worker_id += 1;
        println!("[Preparation] Worker #{} connected, #{} more worker(s) needed!", cur_num, left_num);
        if cur_num + 1 == self.worker_n {
            println!("[Preparation] All worker processes have connected, Map Phase will then begin!");
        }
        ret
    }

    fn report_map_task_finish(self, _: context::Context, id: i32) -> Self::ReportMapTaskFinishFut {
        let cur_map_tasks = self.map_tasks.lock().unwrap();
        let mut cur_map_leases = self.map_leases.lock().unwrap();
        assert!(cur_map_tasks.contains_key(&id) && *cur_map_tasks.get(&id).unwrap() == true);
        assert!(cur_map_leases.contains_key(&id));
        println!("[Map] Map task #{} has been finished", id);
        cur_map_leases.remove_entry(&id);

        for (&k, &v) in &cur_map_tasks.clone() {
            if v {
                continue;
            }
            println!("[Map] deadd map task #{} detected when reporting, the previous worker may have gone offline, will assigned this task to a new worker", k);
            return ready(true);
        }

        if !cur_map_leases.is_empty() {
            println!("[Map] The map lease is not empty, there's still unfinished map tasks");
            return ready(true);
        }

        let mut map_finish = self.map_finish.lock().unwrap();
        let map_id = self.map_id.lock().unwrap();

        if *map_id == self.map_n {
            *map_finish = true;
            println!("[Map] All map tasks have been finished by worker processes, the reduce phase will then begin!");
        }

        ready(true)
    }

    fn report_reduce_task_finish(self, _: context::Context, id: i32) -> Self::ReportReduceTaskFinishFut {
        let cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        let mut cur_reduce_leases = self.reduce_leases.lock().unwrap();
        assert!(cur_reduce_tasks.contains_key(&id) && *cur_reduce_tasks.get(&id).unwrap() == true);
        assert!(cur_reduce_leases.contains_key(&id));
        println!("[Reduce] Reduce task #{} has been finished", id);
        cur_reduce_leases.remove_entry(&id);


        for (&k, &v) in &cur_reduce_tasks.clone() {
            if v {
                continue;
            }
            println!("[Reduce] deadd reduce task #{} detected when reporting, the previous worker may have gone offline, will assigned this task to a new worker", k);
            return ready(true);
        }

        if !cur_reduce_leases.is_empty() {
            println!("[Reduce] The reduce lease is not empty, there's still unfinished reduce tasks");
            return ready(true);
        }

        let mut reduce_finish = self.reduce_finish.lock().unwrap();
        let reduce_id = self.reduce_id.lock().unwrap();
        
        if *reduce_id == self.reduce_n {
            *reduce_finish = true;
            println!("[Reduce] All reduce tasks have been finished by worker processes, MapReduce has finished!");
        }

        ready(true)
    }
}