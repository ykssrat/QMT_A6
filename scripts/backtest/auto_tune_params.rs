//! 参数自动调优 Rust 试验版。
//!
//! 设计目标：
//! 1. 与 auto_tune_params.py 保持尽量一致的命令行接口。
//! 2. 为后续把回测热点逻辑迁移到 Rust 做接口占位。
//! 3. 当前仓库尚未引入 Cargo 工程，因此本文件先作为原型保留。

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TuneArgs {
    pub m_grid: String,
    pub c_grid: String,
    pub h_grid: String,
    pub k_grid: String,
    pub z_grid: String,
    pub y_grid: String,
    pub max_cases: usize,
    pub workers: usize,
    pub apply: bool,
}

#[derive(Debug, Clone)]
pub struct BacktestMetrics {
    pub total_return: f64,
    pub annual_return: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub annual_vol: f64,
    pub win_rate: f64,
}

#[derive(Debug, Clone)]
pub struct ParamCase {
    pub case_idx: usize,
    pub m: f64,
    pub c: f64,
    pub h: f64,
    pub k: f64,
    pub z_threshold: f64,
    pub y_threshold: f64,
}

#[derive(Debug, Clone)]
pub struct TuneOutcome {
    pub case_idx: usize,
    pub params: ParamCase,
    pub metrics: Option<BacktestMetrics>,
    pub score: Option<f64>,
    pub error: Option<String>,
}

pub fn parse_grid(raw: &str) -> Result<Vec<f64>, String> {
    let values: Vec<f64> = raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| item.parse::<f64>().map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    if values.is_empty() {
        return Err("参数列表不能为空".to_string());
    }
    Ok(values)
}

pub fn score(metrics: &BacktestMetrics) -> f64 {
    metrics.total_return + metrics.sharpe_ratio + metrics.win_rate
}

pub fn format_eta(remaining: Duration) -> String {
    let seconds = remaining.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if hours > 0 {
        format!("{}h{:02}m{:02}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m{:02}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

pub fn build_param_cases(args: &TuneArgs) -> Result<Vec<ParamCase>, String> {
    let m_grid = parse_grid(&args.m_grid)?;
    let c_grid = parse_grid(&args.c_grid)?;
    let h_grid = parse_grid(&args.h_grid)?;
    let k_grid = parse_grid(&args.k_grid)?;
    let z_grid = parse_grid(&args.z_grid)?;
    let y_grid = parse_grid(&args.y_grid)?;

    let mut cases = Vec::new();
    let mut idx = 0usize;
    for m in &m_grid {
        for c in &c_grid {
            for h in &h_grid {
                for k in &k_grid {
                    for z in &z_grid {
                        for y in &y_grid {
                            idx += 1;
                            if args.max_cases > 0 && idx > args.max_cases {
                                return Ok(cases);
                            }
                            cases.push(ParamCase {
                                case_idx: idx,
                                m: *m,
                                c: *c,
                                h: *h,
                                k: *k,
                                z_threshold: *z,
                                y_threshold: *y,
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(cases)
}

pub fn evaluate_case_native(case: &ParamCase) -> TuneOutcome {
    // 这里预留给后续 Rust 原生回测核心：
    // 1. 读取缓存后的行情数据
    // 2. 执行 Livermore 逐日回测
    // 3. 返回收益率/夏普/胜率等指标
    TuneOutcome {
        case_idx: case.case_idx,
        params: case.clone(),
        metrics: None,
        score: None,
        error: Some("尚未接入 Rust 原生回测内核，请继续使用 auto_tune_params.py".to_string()),
    }
}

pub fn run_tuning(args: &TuneArgs) -> Result<(), String> {
    let cases = build_param_cases(args)?;
    let started_at = Instant::now();

    println!(
        "开始调优，共 {} 组参数，并发进程数 {}...",
        cases.len(),
        if args.workers == 0 { 1 } else { args.workers }
    );

    for (completed, case) in cases.iter().enumerate() {
        let outcome = evaluate_case_native(case);
        let processed = completed + 1;
        let elapsed = started_at.elapsed();
        let avg_secs = elapsed.as_secs_f64() / processed as f64;
        let remaining = Duration::from_secs_f64(avg_secs * (cases.len().saturating_sub(processed)) as f64);
        let eta = format_eta(remaining);

        if let Some(error) = outcome.error {
            println!(
                "[{}/{}] ETA {}  失败: params={:?}, error={}",
                processed,
                cases.len(),
                eta,
                case,
                error
            );
            continue;
        }
    }

    Ok(())
}