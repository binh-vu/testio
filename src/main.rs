use indicatif::*;
use rayon::prelude::*;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let inputdir = &args[1];
    let n_files = args[2].parse::<usize>().unwrap();
    let exptype = &args[3];

    if exptype == "process" {
        procspawn::init();
    }

    println!("args: {:?}", args);
    let start = Instant::now();

    if exptype == "thread" {
        read_file_thread_par(inputdir, n_files);
    }

    if exptype == "process" {
        read_file_proc_par(inputdir, n_files);
    }

    println!("Main bench takes: {:?}", start.elapsed());
}

fn read_file_thread_par(indir: &str, n_files: usize) {
    let start = Instant::now();
    let res = (0..n_files)
        .into_par_iter()
        .map(|ei| {
            std::fs::read_to_string(&format!("{}/{:0>3}.jl", indir, ei))
                .expect(&format!("{}/{:0>3}.jl", indir, ei))
                .len()
        })
        .progress()
        .collect::<Vec<_>>();
    println!("Read files in parallel takes: {:?}", start.elapsed());
    println!("Num chars: {}", res.into_iter().sum::<usize>())
}

fn read_file_proc_par(indir: &str, n_files: usize) {
    let start = Instant::now();
    let chunks = get_cpus();
    let jobs = (0..chunks)
        .into_iter()
        .map(|idx| {
            let args = (0..n_files)
                .into_iter()
                .filter(|i| i % chunks == idx)
                .map(|ei| format!("{}/{:0>3}.jl", indir, ei))
                .collect::<Vec<_>>();

            procspawn::spawn(args, |args: Vec<String>| {
                args.into_iter()
                    .map(|filename| std::fs::read_to_string(&filename).unwrap().len())
                    .collect::<Vec<_>>()
            })
        })
        .progress()
        .collect::<Vec<_>>();

    let res = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .progress()
        .flatten()
        .collect::<Vec<_>>();
    println!("Fetch files in proc parallel takes: {:?}", start.elapsed());
    println!("Num chars: {}", res.into_iter().sum::<usize>());
}

fn get_cpus() -> usize {
    std::env::var("N_CPUS")
        .unwrap_or(num_cpus::get().to_string())
        .as_str()
        .parse::<usize>()
        .unwrap()
}
