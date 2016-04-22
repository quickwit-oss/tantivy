use time::PreciseTime;
use rustc_serialize::json::ToJson;
use rustc_serialize::json::Json;
use std::collections::BTreeMap;

pub struct TimerHandle<'a> {
    name: &'static str,
    timer_tree: &'a mut TimerTree,
    start: PreciseTime,
    depth: u32,
}

impl<'a> TimerHandle<'a> {
    pub fn open(&mut self, name: &'static str) -> TimerHandle {
        TimerHandle {
            name: name,
            timer_tree: self.timer_tree,
            start: PreciseTime::now(),
            depth: self.depth + 1,
        }
    }
}

impl<'a> Drop for TimerHandle<'a> {
    fn drop(&mut self,) {
        self.timer_tree.timings.push(Timing     {
            name: self.name,
            duration: self.start.to(PreciseTime::now()).num_microseconds().unwrap(),
            depth: self.depth,
        });
    }
}

#[derive(Debug)]
pub struct Timing {
    name: &'static str,
    duration: i64,
    depth: u32,
}

#[derive(Debug)]
pub struct TimerTree {
    timings: Vec<Timing>,
}

impl TimerTree {
    pub fn new() -> TimerTree {
        TimerTree {
            timings: Vec::new(),
        }
    }

    pub fn open(&mut self, name: &'static str) -> TimerHandle {
        TimerHandle {
            name: name,
            timer_tree: self,
            start: PreciseTime::now(),
            depth: 0,
        }
    }
}

fn to_json_obj(timings: &[Timing], root_depth: u32) -> Json {
    let last = timings.len() - 1;
    let last_timing = &timings[last];
    let mut d = BTreeMap::new();
    d.insert("name".to_string(), last_timing.name.to_json());
    d.insert("duration".to_string(), last_timing.duration.to_json());
    if timings.len() > 1 {
        d.insert("children".to_string(), to_json_array(&timings[..last], root_depth + 1));
    }
    Json::Object(d)
}

fn to_json_array(timings: &[Timing], root_depth: u32) -> Json {
    let mut offsets: Vec<usize> = vec!(0);
    for offset in timings.iter()
           .enumerate()
           .filter(|&(offset, timing)| timing.depth == root_depth)
           .map(|(offset, _)| offset) {
               offsets.push(offset + 1);
    }
    let items: Vec<Json> = offsets.iter()
            .zip(offsets[1..].iter())
            .map(|(&start, &stop)| to_json_obj(&timings[start..stop], root_depth))
            .collect();
    Json::Array(items)
}

impl ToJson for TimerTree {
    fn to_json(&self) -> Json {
        to_json_array(&self.timings[..], 0)
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_timer() {
        let mut timer_tree = TimerTree::new();
        {
            let mut a = timer_tree.open("a");
            {
                let mut ab = a.open("b");
                {
                    let abc = ab.open("c");
                }
                {
                    let abd = ab.open("d");
                }
            }
        }
        assert_eq!(timer_tree.timings.len(), 4);
    }
}
