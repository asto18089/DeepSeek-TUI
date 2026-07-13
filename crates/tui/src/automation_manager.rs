//! Durable automation records and scheduler support.
//!
//! Automations are local-first recurring jobs that enqueue standard background
//! tasks. This module stores automation definitions and run history under
//! `~/.codewhale/automations` (or `DEEPSEEK_AUTOMATIONS_DIR` override).

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::task_manager::{NewTaskRequest, SharedTaskManager, TaskStatus};
use crate::utils::spawn_supervised;

const CURRENT_AUTOMATION_SCHEMA_VERSION: u32 = 1;
const CURRENT_RUN_SCHEMA_VERSION: u32 = 1;
const CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION: u32 = 1;
const CURRENT_RUN_INDEX_SCHEMA_VERSION: u32 = 1;
const DEFAULT_MAX_UNPROTECTED_TERMINAL_RUNS: usize = 1_000;
const DEFAULT_AUTOMATION_MODE: &str = "agent";
const DEFAULT_AUTOMATION_ALLOW_SHELL: bool = false;
const DEFAULT_AUTOMATION_TRUST_MODE: bool = false;
const DEFAULT_AUTOMATION_AUTO_APPROVE: bool = false;

const fn default_automation_schema_version() -> u32 {
    CURRENT_AUTOMATION_SCHEMA_VERSION
}

const fn default_run_schema_version() -> u32 {
    CURRENT_RUN_SCHEMA_VERSION
}

const fn default_pending_enqueue_schema_version() -> u32 {
    CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutomationStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutomationRunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRecord {
    #[serde(default = "default_automation_schema_version")]
    pub schema_version: u32,
    pub id: String,
    pub name: String,
    pub prompt: String,
    pub rrule: String,
    #[serde(default)]
    pub cwds: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_shell: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_mode: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_approve: Option<bool>,
    pub status: AutomationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,
}

impl AutomationRecord {
    fn task_mode(&self) -> String {
        self.mode
            .as_deref()
            .map(str::trim)
            .filter(|mode| !mode.is_empty())
            .unwrap_or(DEFAULT_AUTOMATION_MODE)
            .to_string()
    }

    fn task_allow_shell(&self) -> bool {
        self.allow_shell.unwrap_or(DEFAULT_AUTOMATION_ALLOW_SHELL)
    }

    fn task_trust_mode(&self) -> bool {
        self.trust_mode.unwrap_or(DEFAULT_AUTOMATION_TRUST_MODE)
    }

    fn task_auto_approve(&self) -> bool {
        self.auto_approve.unwrap_or(DEFAULT_AUTOMATION_AUTO_APPROVE)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRunRecord {
    #[serde(default = "default_run_schema_version")]
    pub schema_version: u32,
    pub id: String,
    pub automation_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub status: AutomationRunStatus,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Host hook for terminal runs whose linked conversations or other external
/// resources still require the run record to remain addressable.
pub trait AutomationRunRetentionGuard: Send + Sync {
    fn retain_terminal_run(&self, run: &AutomationRunRecord) -> Result<bool>;
}

#[derive(Clone)]
pub struct AutomationManagerOptions {
    pub max_unprotected_terminal_runs: usize,
    pub retention_guard: Option<Arc<dyn AutomationRunRetentionGuard>>,
}

impl Default for AutomationManagerOptions {
    fn default() -> Self {
        Self {
            max_unprotected_terminal_runs: DEFAULT_MAX_UNPROTECTED_TERMINAL_RUNS,
            retention_guard: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AutomationRunIndex {
    schema_version: u32,
    automation_id: String,
    entries: BTreeMap<String, AutomationRunIndexEntry>,
    authority_generation: RunAuthorityGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_terminal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RunAuthorityGeneration {
    modified_secs: u64,
    modified_nanos: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AutomationRunIndexEntry {
    created_at: DateTime<Utc>,
    status: AutomationRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_at: Option<DateTime<Utc>>,
}

impl AutomationRunIndexEntry {
    fn from_run(run: &AutomationRunRecord) -> Self {
        Self {
            created_at: run.created_at,
            status: run.status,
            terminal_at: is_terminal_run_status(run.status)
                .then_some(run.ended_at)
                .flatten(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingEnqueueRecord {
    #[serde(default = "default_pending_enqueue_schema_version")]
    schema_version: u32,
    #[serde(default = "default_pending_enqueue_kind")]
    kind: PendingEnqueueKind,
    slot_key: String,
    run: AutomationRunRecord,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PendingEnqueueKind {
    Scheduled,
    Manual,
}

const fn default_pending_enqueue_kind() -> PendingEnqueueKind {
    PendingEnqueueKind::Scheduled
}

impl PendingEnqueueRecord {
    fn for_slot(
        automation_id: &str,
        scheduled_for: DateTime<Utc>,
        created_at: DateTime<Utc>,
    ) -> Self {
        let slot_timestamp = scheduled_for.timestamp_micros();
        Self {
            schema_version: CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION,
            kind: PendingEnqueueKind::Scheduled,
            slot_key: format!("automation:{automation_id}:slot:{slot_timestamp}"),
            run: AutomationRunRecord {
                schema_version: CURRENT_RUN_SCHEMA_VERSION,
                id: format!("slot_{slot_timestamp}"),
                automation_id: automation_id.to_string(),
                scheduled_for,
                status: AutomationRunStatus::Queued,
                created_at,
                started_at: None,
                ended_at: None,
                task_id: None,
                thread_id: None,
                turn_id: None,
                error: None,
            },
        }
    }

    fn for_manual(
        automation_id: &str,
        invocation_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<Self> {
        let invocation_id = Uuid::parse_str(invocation_id)
            .with_context(|| format!("Invalid manual run invocation id '{invocation_id}'"))?
            .to_string();
        Ok(Self {
            schema_version: CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION,
            kind: PendingEnqueueKind::Manual,
            slot_key: format!("automation:{automation_id}:manual:{invocation_id}"),
            run: AutomationRunRecord {
                schema_version: CURRENT_RUN_SCHEMA_VERSION,
                id: format!("manual_{invocation_id}"),
                automation_id: automation_id.to_string(),
                scheduled_for: created_at,
                status: AutomationRunStatus::Queued,
                created_at,
                started_at: None,
                ended_at: None,
                task_id: None,
                thread_id: None,
                turn_id: None,
                error: None,
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAutomationRequest {
    pub name: String,
    pub prompt: String,
    pub rrule: String,
    #[serde(default)]
    pub cwds: Vec<PathBuf>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub allow_shell: Option<bool>,
    #[serde(default)]
    pub trust_mode: Option<bool>,
    #[serde(default)]
    pub auto_approve: Option<bool>,
    #[serde(default)]
    pub status: Option<AutomationStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateAutomationRequest {
    pub name: Option<String>,
    pub prompt: Option<String>,
    pub rrule: Option<String>,
    pub cwds: Option<Vec<PathBuf>>,
    pub model: Option<String>,
    pub mode: Option<String>,
    pub allow_shell: Option<bool>,
    pub trust_mode: Option<bool>,
    pub auto_approve: Option<bool>,
    pub status: Option<AutomationStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutomationFrequency {
    Minutely,
    Hourly,
    Weekly,
}

#[derive(Debug, Clone)]
pub enum AutomationSchedule {
    Minutely {
        interval_minutes: u32,
    },
    Hourly {
        interval_hours: u32,
        byday: Option<Vec<Weekday>>,
    },
    Weekly {
        byday: Vec<Weekday>,
        byhour: u32,
        byminute: u32,
    },
}

impl AutomationSchedule {
    pub fn parse_rrule(rrule: &str) -> Result<Self> {
        let mut parts: BTreeMap<String, String> = BTreeMap::new();
        for raw in rrule.split(';') {
            let item = raw.trim();
            if item.is_empty() {
                continue;
            }
            let Some((k, v)) = item.split_once('=') else {
                bail!("Invalid RRULE segment '{item}'");
            };
            parts.insert(k.trim().to_ascii_uppercase(), v.trim().to_ascii_uppercase());
        }

        let freq = match parts.get("FREQ").map(String::as_str) {
            Some("MINUTELY") => AutomationFrequency::Minutely,
            Some("HOURLY") => AutomationFrequency::Hourly,
            Some("WEEKLY") => AutomationFrequency::Weekly,
            Some(other) => {
                bail!("Unsupported RRULE FREQ '{other}'. Supported: MINUTELY, HOURLY and WEEKLY")
            }
            None => bail!("RRULE must include FREQ"),
        };

        match freq {
            AutomationFrequency::Minutely => {
                for key in parts.keys() {
                    if key != "FREQ" && key != "INTERVAL" {
                        bail!(
                            "Unsupported RRULE field '{key}' for MINUTELY. Allowed: FREQ,INTERVAL"
                        );
                    }
                }
                let interval_minutes = parts
                    .get("INTERVAL")
                    .map(|v| v.parse::<u32>())
                    .transpose()
                    .context("Failed to parse INTERVAL")?
                    .unwrap_or(1);
                if interval_minutes == 0 {
                    bail!("INTERVAL must be >= 1 for MINUTELY schedules");
                }
                Ok(Self::Minutely { interval_minutes })
            }
            AutomationFrequency::Hourly => {
                for key in parts.keys() {
                    if key != "FREQ" && key != "INTERVAL" && key != "BYDAY" {
                        bail!(
                            "Unsupported RRULE field '{key}' for HOURLY. Allowed: FREQ,INTERVAL,BYDAY"
                        );
                    }
                }
                let interval_hours = parts
                    .get("INTERVAL")
                    .map(|v| v.parse::<u32>())
                    .transpose()
                    .context("Failed to parse INTERVAL")?
                    .unwrap_or(1);
                if interval_hours == 0 {
                    bail!("INTERVAL must be >= 1 for HOURLY schedules");
                }
                let byday = parts
                    .get("BYDAY")
                    .map(|value| parse_byday(value))
                    .transpose()?;
                Ok(Self::Hourly {
                    interval_hours,
                    byday,
                })
            }
            AutomationFrequency::Weekly => {
                for key in parts.keys() {
                    if key != "FREQ" && key != "BYDAY" && key != "BYHOUR" && key != "BYMINUTE" {
                        bail!(
                            "Unsupported RRULE field '{key}' for WEEKLY. Allowed: FREQ,BYDAY,BYHOUR,BYMINUTE"
                        );
                    }
                }
                let byday_raw = parts
                    .get("BYDAY")
                    .ok_or_else(|| anyhow::anyhow!("WEEKLY schedules require BYDAY"))?;
                let byday = parse_byday(byday_raw)?;
                if byday.is_empty() {
                    bail!("BYDAY cannot be empty for WEEKLY schedules");
                }
                let byhour = parts
                    .get("BYHOUR")
                    .ok_or_else(|| anyhow::anyhow!("WEEKLY schedules require BYHOUR"))?
                    .parse::<u32>()
                    .context("Failed to parse BYHOUR")?;
                let byminute = parts
                    .get("BYMINUTE")
                    .ok_or_else(|| anyhow::anyhow!("WEEKLY schedules require BYMINUTE"))?
                    .parse::<u32>()
                    .context("Failed to parse BYMINUTE")?;

                if byhour > 23 {
                    bail!("BYHOUR must be between 0 and 23");
                }
                if byminute > 59 {
                    bail!("BYMINUTE must be between 0 and 59");
                }

                Ok(Self::Weekly {
                    byday,
                    byhour,
                    byminute,
                })
            }
        }
    }

    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let local_after = after.with_timezone(&Local);
        match self {
            Self::Minutely { interval_minutes } => {
                let candidate = local_after + Duration::minutes(i64::from(*interval_minutes))
                    - Duration::seconds(i64::from(local_after.second()))
                    - Duration::nanoseconds(i64::from(local_after.nanosecond()));

                Ok(candidate.with_timezone(&Utc))
            }
            Self::Hourly {
                interval_hours,
                byday,
            } => {
                let mut candidate = local_after + Duration::hours(i64::from(*interval_hours))
                    - Duration::seconds(i64::from(local_after.second()))
                    - Duration::nanoseconds(i64::from(local_after.nanosecond()));

                if let Some(days) = byday {
                    for _ in 0..(24 * 21) {
                        if days.contains(&candidate.weekday()) {
                            return Ok(candidate.with_timezone(&Utc));
                        }
                        candidate += Duration::hours(i64::from(*interval_hours));
                    }
                    bail!("Unable to compute next HOURLY run for BYDAY filter");
                }

                Ok(candidate.with_timezone(&Utc))
            }
            Self::Weekly {
                byday,
                byhour,
                byminute,
            } => {
                for day_offset in 0..15 {
                    let date = local_after.date_naive() + Duration::days(i64::from(day_offset));
                    if !byday.contains(&date.weekday()) {
                        continue;
                    }
                    let Some(candidate_naive) = date.and_hms_opt(*byhour, *byminute, 0) else {
                        continue;
                    };
                    if let Some(candidate) = resolve_local_datetime(candidate_naive)
                        && candidate > local_after
                    {
                        return Ok(candidate.with_timezone(&Utc));
                    }
                }
                bail!("Unable to compute next WEEKLY run");
            }
        }
    }

    fn normalize_due_cursor(&self, cursor: DateTime<Utc>) -> DateTime<Utc> {
        if !matches!(self, Self::Minutely { .. })
            || (cursor.second() == 0 && cursor.nanosecond() == 0)
        {
            return cursor;
        }

        cursor + Duration::minutes(1)
            - Duration::seconds(i64::from(cursor.second()))
            - Duration::nanoseconds(i64::from(cursor.nanosecond()))
    }

    fn next_due_after(
        &self,
        first_due: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        let first_due = self.normalize_due_cursor(first_due);
        if first_due > now {
            return Ok(first_due);
        }
        let latest_due = self.latest_due_at_or_before(first_due, now)?;
        self.next_after(latest_due)
    }

    fn latest_due_at_or_before(
        &self,
        first_due: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        let first_due = self.normalize_due_cursor(first_due);
        if first_due >= now {
            return Ok(first_due);
        }

        match self {
            Self::Minutely { interval_minutes } => {
                let interval = i64::from(*interval_minutes);
                let elapsed_minutes = (now - first_due).num_minutes().max(0);
                Ok(first_due + Duration::minutes((elapsed_minutes / interval) * interval))
            }
            Self::Hourly {
                interval_hours,
                byday,
            } => {
                let interval = i64::from(*interval_hours);
                let elapsed_hours = (now - first_due).num_hours().max(0);
                let mut candidate =
                    first_due + Duration::hours((elapsed_hours / interval) * interval);

                if let Some(days) = byday {
                    for _ in 0..(24 * 21) {
                        if days.contains(&candidate.with_timezone(&Local).weekday()) {
                            return Ok(candidate);
                        }
                        if candidate <= first_due {
                            return Ok(first_due);
                        }
                        candidate -= Duration::hours(interval);
                    }
                    bail!("Unable to fast-forward HOURLY run for BYDAY filter");
                }

                Ok(candidate)
            }
            Self::Weekly { .. } => {
                let mut latest = first_due;
                loop {
                    let next = self.next_after(latest)?;
                    if next > now {
                        return Ok(latest);
                    }
                    latest = next;
                }
            }
        }
    }
}

fn resolve_local_datetime(naive: chrono::NaiveDateTime) -> Option<DateTime<Local>> {
    Local
        .from_local_datetime(&naive)
        .single()
        .or_else(|| Local.from_local_datetime(&naive).earliest())
        .or_else(|| Local.from_local_datetime(&naive).latest())
}

fn storage_owner_directories(root: &Path) -> Result<Vec<String>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut owners = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("Failed to read {}", root.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let owner = entry
            .file_name()
            .to_str()
            .map(str::to_string)
            .with_context(|| {
                format!(
                    "Storage owner directory must be valid UTF-8: {}",
                    entry.path().display()
                )
            })?;
        ensure_safe_storage_id("automation id", &owner)?;
        owners.push(owner);
    }
    owners.sort();
    Ok(owners)
}

fn parse_byday(value: &str) -> Result<Vec<Weekday>> {
    let mut days = Vec::new();
    for token in value.split(',') {
        let day = match token.trim().to_ascii_uppercase().as_str() {
            "MO" => Weekday::Mon,
            "TU" => Weekday::Tue,
            "WE" => Weekday::Wed,
            "TH" => Weekday::Thu,
            "FR" => Weekday::Fri,
            "SA" => Weekday::Sat,
            "SU" => Weekday::Sun,
            other => bail!("Invalid BYDAY value '{other}'"),
        };
        if !days.contains(&day) {
            days.push(day);
        }
    }
    Ok(days)
}

#[derive(Clone)]
pub struct AutomationManager {
    automations_dir: PathBuf,
    runs_dir: PathBuf,
    pending_dir: PathBuf,
    options: AutomationManagerOptions,
    index_gate: Arc<StdMutex<()>>,
    forced_dirty_indexes: Arc<StdMutex<HashSet<String>>>,
    #[cfg(test)]
    fail_next_automation_save: Arc<StdMutex<bool>>,
    #[cfg(test)]
    fail_next_run_save: Arc<StdMutex<bool>>,
    #[cfg(test)]
    run_io_probe: Arc<RunIoProbe>,
}

#[cfg(test)]
#[derive(Default)]
struct RunIoProbe {
    authority_reads: AtomicUsize,
}

impl AutomationManager {
    pub fn open(root: PathBuf) -> Result<Self> {
        Self::open_with_options(root, AutomationManagerOptions::default())
    }

    pub fn open_with_options(root: PathBuf, options: AutomationManagerOptions) -> Result<Self> {
        let automations_dir = root.join("automations");
        let runs_dir = root.join("runs");
        let pending_dir = root.join("pending");
        fs::create_dir_all(&automations_dir)
            .with_context(|| format!("Failed to create {}", automations_dir.display()))?;
        fs::create_dir_all(&runs_dir)
            .with_context(|| format!("Failed to create {}", runs_dir.display()))?;
        fs::create_dir_all(&pending_dir)
            .with_context(|| format!("Failed to create {}", pending_dir.display()))?;
        Ok(Self {
            automations_dir,
            runs_dir,
            pending_dir,
            options,
            index_gate: Arc::new(StdMutex::new(())),
            forced_dirty_indexes: Arc::new(StdMutex::new(HashSet::new())),
            #[cfg(test)]
            fail_next_automation_save: Arc::new(StdMutex::new(false)),
            #[cfg(test)]
            fail_next_run_save: Arc::new(StdMutex::new(false)),
            #[cfg(test)]
            run_io_probe: Arc::new(RunIoProbe::default()),
        })
    }

    pub fn default_location() -> Result<Self> {
        Self::open(default_automations_dir())
    }

    fn automation_path(&self, id: &str) -> Result<PathBuf> {
        ensure_safe_storage_id("automation id", id)?;
        Ok(self.automations_dir.join(format!("{id}.json")))
    }

    fn runs_dir_for(&self, automation_id: &str) -> Result<PathBuf> {
        ensure_safe_storage_id("automation id", automation_id)?;
        Ok(self.runs_dir.join(automation_id))
    }

    fn run_path(&self, automation_id: &str, run_id: &str) -> Result<PathBuf> {
        ensure_safe_storage_id("run id", run_id)?;
        Ok(self
            .runs_dir_for(automation_id)?
            .join(format!("{run_id}.json")))
    }

    fn run_index_dir_for(&self, automation_id: &str) -> Result<PathBuf> {
        Ok(self.runs_dir_for(automation_id)?.join(".index"))
    }

    fn run_index_path(&self, automation_id: &str) -> Result<PathBuf> {
        Ok(self.run_index_dir_for(automation_id)?.join("v1.json"))
    }

    fn run_index_dirty_path(&self, automation_id: &str) -> Result<PathBuf> {
        Ok(self.run_index_dir_for(automation_id)?.join("dirty"))
    }

    fn run_authority_generation(&self, automation_id: &str) -> Result<RunAuthorityGeneration> {
        let dir = self.runs_dir_for(automation_id)?;
        let modified = fs::metadata(&dir)
            .with_context(|| format!("Failed to inspect {}", dir.display()))?
            .modified()
            .with_context(|| format!("Failed to read modification time for {}", dir.display()))?;
        let elapsed = modified.duration_since(UNIX_EPOCH).with_context(|| {
            format!(
                "Run directory timestamp predates the Unix epoch: {}",
                dir.display()
            )
        })?;
        Ok(RunAuthorityGeneration {
            modified_secs: elapsed.as_secs(),
            modified_nanos: elapsed.subsec_nanos(),
        })
    }

    fn pending_dir_for(&self, automation_id: &str) -> Result<PathBuf> {
        ensure_safe_storage_id("automation id", automation_id)?;
        Ok(self.pending_dir.join(automation_id))
    }

    fn pending_path(&self, automation_id: &str, run_id: &str) -> Result<PathBuf> {
        ensure_safe_storage_id("run id", run_id)?;
        Ok(self
            .pending_dir_for(automation_id)?
            .join(format!("{run_id}.json")))
    }

    pub fn create_automation(&self, req: CreateAutomationRequest) -> Result<AutomationRecord> {
        validate_name_and_prompt(&req.name, &req.prompt)?;
        let schedule = AutomationSchedule::parse_rrule(&req.rrule)?;
        let now = Utc::now();
        let status = req.status.unwrap_or(AutomationStatus::Active);
        let next_run_at = if matches!(status, AutomationStatus::Active) {
            Some(schedule.next_after(now)?)
        } else {
            None
        };

        let record = AutomationRecord {
            schema_version: CURRENT_AUTOMATION_SCHEMA_VERSION,
            id: Uuid::new_v4().to_string(),
            name: req.name.trim().to_string(),
            prompt: req.prompt.trim().to_string(),
            rrule: req.rrule.trim().to_ascii_uppercase(),
            cwds: req.cwds,
            model: normalize_optional_string(req.model),
            mode: normalize_optional_mode(req.mode)?,
            allow_shell: req.allow_shell,
            trust_mode: req.trust_mode,
            auto_approve: req.auto_approve,
            status,
            created_at: now,
            updated_at: now,
            next_run_at,
            last_run_at: None,
        };

        self.save_automation(&record)?;
        Ok(record)
    }

    pub fn get_automation(&self, id: &str) -> Result<AutomationRecord> {
        let path = self.automation_path(id)?;
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read automation {}", path.display()))?;
        let mut record: AutomationRecord = serde_json::from_str(&raw)
            .with_context(|| format!("Failed to parse automation {}", path.display()))?;
        if record.schema_version > CURRENT_AUTOMATION_SCHEMA_VERSION {
            bail!(
                "Automation schema v{} is newer than supported v{}",
                record.schema_version,
                CURRENT_AUTOMATION_SCHEMA_VERSION
            );
        }
        if normalize_persisted_mode(&mut record)? {
            write_json_atomic(&path, &record)?;
        }
        validate_automation_record(&record, id)?;
        Ok(record)
    }

    pub fn save_automation(&self, record: &AutomationRecord) -> Result<()> {
        validate_automation_record(record, &record.id)?;
        #[cfg(test)]
        {
            let mut fail_next = self
                .fail_next_automation_save
                .lock()
                .map_err(|_| anyhow::anyhow!("Automation save failpoint lock is poisoned"))?;
            if std::mem::take(&mut *fail_next) {
                bail!("injected automation save failure");
            }
        }
        write_json_atomic(&self.automation_path(&record.id)?, record)
    }

    #[cfg(test)]
    fn fail_next_automation_save(&self) {
        *self
            .fail_next_automation_save
            .lock()
            .expect("automation save failpoint lock") = true;
    }

    #[cfg(test)]
    fn fail_next_run_save(&self) {
        *self
            .fail_next_run_save
            .lock()
            .expect("run save failpoint lock") = true;
    }

    #[cfg(test)]
    fn reset_run_io_probe(&self) {
        self.run_io_probe.authority_reads.store(0, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn run_authority_read_count(&self) -> usize {
        self.run_io_probe.authority_reads.load(Ordering::SeqCst)
    }

    pub fn list_automations(&self) -> Result<Vec<AutomationRecord>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.automations_dir)
            .with_context(|| format!("Failed to read {}", self.automations_dir.display()))?
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    tracing::warn!(error = %err, "skipping unreadable automation directory entry");
                    continue;
                }
            };
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let loaded = (|| -> Result<AutomationRecord> {
                let file_stem = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .context("Automation filename must be valid UTF-8")?;
                let raw = fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?;
                let mut record: AutomationRecord = serde_json::from_str(&raw)
                    .with_context(|| format!("Failed to parse {}", path.display()))?;
                if record.schema_version > CURRENT_AUTOMATION_SCHEMA_VERSION {
                    bail!(
                        "Automation schema v{} is newer than supported v{}",
                        record.schema_version,
                        CURRENT_AUTOMATION_SCHEMA_VERSION
                    );
                }
                if normalize_persisted_mode(&mut record)? {
                    write_json_atomic(&path, &record)?;
                }
                validate_automation_record(&record, file_stem)?;
                Ok(record)
            })();
            match loaded {
                Ok(record) => out.push(record),
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "skipping invalid automation record");
                }
            }
        }
        out.sort_by_key(|r| std::cmp::Reverse(r.updated_at));
        Ok(out)
    }

    pub fn update_automation(
        &self,
        id: &str,
        req: UpdateAutomationRequest,
    ) -> Result<AutomationRecord> {
        let mut existing = self.get_automation(id)?;

        if let Some(name) = req.name {
            if name.trim().is_empty() {
                bail!("Automation name cannot be empty");
            }
            existing.name = name.trim().to_string();
        }
        if let Some(prompt) = req.prompt {
            if prompt.trim().is_empty() {
                bail!("Automation prompt cannot be empty");
            }
            existing.prompt = prompt.trim().to_string();
        }
        if let Some(rrule) = req.rrule {
            let normalized = rrule.trim().to_ascii_uppercase();
            AutomationSchedule::parse_rrule(&normalized)?;
            existing.rrule = normalized;
            if matches!(existing.status, AutomationStatus::Active) {
                let schedule = AutomationSchedule::parse_rrule(&existing.rrule)?;
                existing.next_run_at = Some(schedule.next_after(Utc::now())?);
            }
        }
        if let Some(cwds) = req.cwds {
            existing.cwds = cwds;
        }
        if let Some(model) = req.model {
            existing.model = normalize_optional_string(Some(model));
        }
        if let Some(mode) = req.mode {
            existing.mode = normalize_optional_mode(Some(mode))?;
        }
        if let Some(allow_shell) = req.allow_shell {
            existing.allow_shell = Some(allow_shell);
        }
        if let Some(trust_mode) = req.trust_mode {
            existing.trust_mode = Some(trust_mode);
        }
        if let Some(auto_approve) = req.auto_approve {
            existing.auto_approve = Some(auto_approve);
        }
        if let Some(status) = req.status {
            existing.status = status;
            if matches!(status, AutomationStatus::Paused) {
                existing.next_run_at = None;
            } else {
                let schedule = AutomationSchedule::parse_rrule(&existing.rrule)?;
                existing.next_run_at = Some(schedule.next_after(Utc::now())?);
            }
        }

        existing.updated_at = Utc::now();
        self.save_automation(&existing)?;
        Ok(existing)
    }

    pub fn pause_automation(&self, id: &str) -> Result<AutomationRecord> {
        self.update_automation(
            id,
            UpdateAutomationRequest {
                status: Some(AutomationStatus::Paused),
                ..UpdateAutomationRequest::default()
            },
        )
    }

    pub fn resume_automation(&self, id: &str) -> Result<AutomationRecord> {
        self.update_automation(
            id,
            UpdateAutomationRequest {
                status: Some(AutomationStatus::Active),
                ..UpdateAutomationRequest::default()
            },
        )
    }

    pub fn delete_automation(&self, id: &str) -> Result<AutomationRecord> {
        let existing = self.get_automation(id)?;
        let path = self.automation_path(id)?;
        fs::remove_file(&path)
            .with_context(|| format!("Failed to delete automation {}", path.display()))?;

        let runs_dir = self.runs_dir_for(id)?;
        if runs_dir.exists() {
            fs::remove_dir_all(&runs_dir).with_context(|| {
                format!("Failed to delete automation runs {}", runs_dir.display())
            })?;
        }
        let pending_dir = self.pending_dir_for(id)?;
        if pending_dir.exists() {
            fs::remove_dir_all(&pending_dir).with_context(|| {
                format!(
                    "Failed to delete pending automation runs {}",
                    pending_dir.display()
                )
            })?;
        }

        Ok(existing)
    }

    pub fn list_runs(
        &self,
        automation_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<AutomationRunRecord>> {
        self.runs_dir_for(automation_id)?;
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let _gate = self
            .index_gate
            .lock()
            .map_err(|_| anyhow::anyhow!("Automation run index lock is poisoned"))?;
        let index = self.load_or_rebuild_run_index_locked(automation_id)?;
        match self.load_indexed_runs_locked(automation_id, &index, limit) {
            Ok(runs) => Ok(runs),
            Err(first_err) => {
                if let Err(err) = self.mark_run_index_dirty_locked(automation_id) {
                    tracing::warn!(automation_id, error = %err, "failed to mark stale run index dirty");
                }
                let rebuilt = self
                    .rebuild_run_index_locked(automation_id)
                    .with_context(|| format!("Run index was stale: {first_err}"))?;
                self.load_indexed_runs_locked(automation_id, &rebuilt, limit)
            }
        }
    }

    /// Collect task ids that must survive task-manager pruning.
    ///
    /// This includes every retained run and every valid pending-enqueue
    /// journal. Any unreadable or invalid pending journal fails closed so a
    /// caller can skip the entire prune rather than orphaning crash recovery.
    pub fn protected_task_ids(&self) -> Result<HashSet<String>> {
        let mut protected = HashSet::new();

        for automation_id in storage_owner_directories(&self.pending_dir)? {
            let (pending, blocked) = self.scan_pending_enqueues(&automation_id)?;
            if blocked {
                bail!(
                    "Task pruning blocked by invalid pending enqueue records for automation '{}'",
                    automation_id
                );
            }
            protected.extend(pending.into_iter().filter_map(|record| record.run.task_id));
        }

        for automation_id in storage_owner_directories(&self.runs_dir)? {
            protected.extend(
                self.list_runs(&automation_id, None)?
                    .into_iter()
                    .filter_map(|run| run.task_id),
            );
        }

        Ok(protected)
    }

    fn load_indexed_runs_locked(
        &self,
        automation_id: &str,
        index: &AutomationRunIndex,
        limit: Option<usize>,
    ) -> Result<Vec<AutomationRunRecord>> {
        let mut entries = index.entries.iter().collect::<Vec<_>>();
        entries.sort_by(|(left_id, left), (right_id, right)| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left_id.cmp(right_id))
        });
        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        let mut out = Vec::new();
        for (run_id, entry) in entries {
            let Some(run) = self.load_run(automation_id, run_id)? else {
                bail!("Run index references missing run '{run_id}'");
            };
            if run.created_at != entry.created_at
                || run.status != entry.status
                || (is_terminal_run_status(run.status) && run.ended_at != entry.terminal_at)
            {
                bail!(
                    "Run index metadata for '{}' does not match its authority record",
                    run.id
                );
            }
            out.push(run);
        }
        Ok(out)
    }

    fn save_run(&self, run: &AutomationRunRecord) -> Result<()> {
        validate_run_record(run, &run.automation_id, &run.id)?;
        #[cfg(test)]
        {
            let mut fail_next = self
                .fail_next_run_save
                .lock()
                .map_err(|_| anyhow::anyhow!("Run save failpoint lock is poisoned"))?;
            if std::mem::take(&mut *fail_next) {
                bail!("injected run save failure");
            }
        }
        let _gate = self
            .index_gate
            .lock()
            .map_err(|_| anyhow::anyhow!("Automation run index lock is poisoned"))?;
        let mut index = match self.load_or_rebuild_run_index_locked(&run.automation_id) {
            Ok(index) => Some(index),
            Err(err) => {
                tracing::warn!(automation_id = %run.automation_id, error = %err, "run index unavailable before authority write");
                None
            }
        };
        self.mark_run_index_dirty_locked(&run.automation_id)?;
        let dir = self.runs_dir_for(&run.automation_id)?;
        fs::create_dir_all(&dir).with_context(|| format!("Failed to create {}", dir.display()))?;
        write_json_atomic(&self.run_path(&run.automation_id, &run.id)?, run)?;

        if let Some(index) = index.as_mut() {
            index
                .entries
                .insert(run.id.clone(), AutomationRunIndexEntry::from_run(run));
            if is_terminal_run_status(run.status) {
                self.apply_run_retention_locked(&run.automation_id, index, None);
            }
            refresh_latest_terminal_at(index);
            if let Err(err) = self.persist_run_index_locked(&run.automation_id, index) {
                tracing::warn!(automation_id = %run.automation_id, error = %err, "authority run saved but sidecar update failed");
            }
        } else if let Err(err) = self.rebuild_run_index_locked(&run.automation_id) {
            tracing::warn!(automation_id = %run.automation_id, error = %err, "authority run saved but sidecar rebuild failed");
        }
        Ok(())
    }

    fn load_run(&self, automation_id: &str, run_id: &str) -> Result<Option<AutomationRunRecord>> {
        let path = self.run_path(automation_id, run_id)?;
        match fs::read_to_string(&path) {
            Ok(raw) => {
                #[cfg(test)]
                self.run_io_probe
                    .authority_reads
                    .fetch_add(1, Ordering::SeqCst);
                read_run_json(&path, &raw, automation_id, run_id).map(Some)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => Err(err).with_context(|| format!("Failed to read {}", path.display())),
        }
    }

    fn load_or_rebuild_run_index_locked(&self, automation_id: &str) -> Result<AutomationRunIndex> {
        if !self.run_index_is_dirty_locked(automation_id)? {
            let path = self.run_index_path(automation_id)?;
            match fs::read_to_string(&path) {
                Ok(raw) => {
                    match serde_json::from_str::<AutomationRunIndex>(&raw)
                        .with_context(|| format!("Failed to parse {}", path.display()))
                        .and_then(|index| {
                            validate_run_index(&index, automation_id)?;
                            if index.authority_generation
                                != self.run_authority_generation(automation_id)?
                            {
                                bail!("Run authority directory changed since the index was saved");
                            }
                            Ok(index)
                        }) {
                        Ok(index) => return Ok(index),
                        Err(err) => {
                            tracing::warn!(automation_id, error = %err, "rebuilding invalid run index");
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    tracing::warn!(automation_id, error = %err, "rebuilding unreadable run index");
                }
            }
        }
        self.rebuild_run_index_locked(automation_id)
    }

    fn rebuild_run_index_locked(&self, automation_id: &str) -> Result<AutomationRunIndex> {
        self.mark_run_index_dirty_locked(automation_id)?;
        let dir = self.runs_dir_for(automation_id)?;
        let mut cached_runs = BTreeMap::new();
        if dir.exists() {
            for entry in
                fs::read_dir(&dir).with_context(|| format!("Failed to read {}", dir.display()))?
            {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_none_or(|extension| extension != "json") {
                    continue;
                }
                let run_id = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .context("Run filename must be valid UTF-8")?;
                let raw = fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?;
                #[cfg(test)]
                self.run_io_probe
                    .authority_reads
                    .fetch_add(1, Ordering::SeqCst);
                let run = read_run_json(&path, &raw, automation_id, run_id)?;
                cached_runs.insert(run.id.clone(), run);
            }
        }
        let mut index = AutomationRunIndex {
            schema_version: CURRENT_RUN_INDEX_SCHEMA_VERSION,
            automation_id: automation_id.to_string(),
            entries: cached_runs
                .iter()
                .map(|(id, run)| (id.clone(), AutomationRunIndexEntry::from_run(run)))
                .collect(),
            authority_generation: RunAuthorityGeneration::default(),
            latest_terminal_at: None,
        };
        self.apply_run_retention_locked(automation_id, &mut index, Some(&cached_runs));
        refresh_latest_terminal_at(&mut index);
        if let Err(err) = self.persist_run_index_locked(automation_id, &mut index) {
            tracing::warn!(automation_id, error = %err, "rebuilt run index remains dirty because sidecar persistence failed");
        }
        Ok(index)
    }

    fn apply_run_retention_locked(
        &self,
        automation_id: &str,
        index: &mut AutomationRunIndex,
        cached_runs: Option<&BTreeMap<String, AutomationRunRecord>>,
    ) {
        let mut terminal = index
            .entries
            .iter()
            .filter(|(_, entry)| is_terminal_run_status(entry.status))
            .map(|(id, entry)| (id.clone(), entry.created_at))
            .collect::<Vec<_>>();
        if terminal.len() <= self.options.max_unprotected_terminal_runs {
            return;
        }
        terminal.sort_by(|(left_id, left_at), (right_id, right_at)| {
            right_at.cmp(left_at).then_with(|| left_id.cmp(right_id))
        });
        let prune_candidates = terminal.split_off(self.options.max_unprotected_terminal_runs);

        let (pending, blocked) = match self.scan_pending_enqueues(automation_id) {
            Ok(scan) => scan,
            Err(err) => {
                tracing::warn!(automation_id, error = %err, "run retention blocked because pending ownership is unreadable");
                return;
            }
        };
        if blocked {
            tracing::warn!(
                automation_id,
                "run retention blocked by invalid pending enqueue records"
            );
            return;
        }
        let pending_ids = pending
            .into_iter()
            .map(|record| record.run.id)
            .collect::<HashSet<_>>();

        let mut host_protected = HashSet::new();
        if let Some(guard) = self.options.retention_guard.as_ref() {
            for (run_id, _) in &prune_candidates {
                if pending_ids.contains(run_id) {
                    continue;
                }
                let run = match cached_runs.and_then(|runs| runs.get(run_id)).cloned() {
                    Some(run) => run,
                    None => match self.load_run(automation_id, run_id) {
                        Ok(Some(run)) => run,
                        Ok(None) => {
                            tracing::warn!(
                                automation_id,
                                run_id,
                                "run retention blocked by missing authority record"
                            );
                            return;
                        }
                        Err(err) => {
                            tracing::warn!(automation_id, run_id, error = %err, "run retention guard could not inspect authority record");
                            return;
                        }
                    },
                };
                match guard.retain_terminal_run(&run) {
                    Ok(true) => {
                        host_protected.insert(run_id.clone());
                    }
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(automation_id, run_id, error = %err, "run retention guard failed; retaining all runs for owner");
                        return;
                    }
                }
            }
        }

        let mut prune = Vec::new();
        for (run_id, _) in prune_candidates {
            if pending_ids.contains(&run_id) || host_protected.contains(&run_id) {
                continue;
            }
            prune.push(run_id);
        }
        for run_id in prune {
            let path = match self.run_path(automation_id, &run_id) {
                Ok(path) => path,
                Err(err) => {
                    tracing::warn!(automation_id, run_id, error = %err, "failed to resolve prunable run path");
                    continue;
                }
            };
            match fs::remove_file(&path) {
                Ok(()) => {
                    index.entries.remove(&run_id);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    index.entries.remove(&run_id);
                }
                Err(err) => {
                    tracing::warn!(automation_id, run_id, error = %err, "failed to prune terminal run");
                }
            }
        }
    }

    fn persist_run_index_locked(
        &self,
        automation_id: &str,
        index: &mut AutomationRunIndex,
    ) -> Result<()> {
        index.authority_generation = self.run_authority_generation(automation_id)?;
        let path = self.run_index_path(automation_id)?;
        write_json_atomic(&path, index)?;
        self.clear_run_index_dirty_locked(automation_id)
    }

    fn mark_run_index_dirty_locked(&self, automation_id: &str) -> Result<()> {
        self.forced_dirty_indexes
            .lock()
            .map_err(|_| anyhow::anyhow!("Automation dirty-index lock is poisoned"))?
            .insert(automation_id.to_string());
        let path = self.run_index_dirty_path(automation_id)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}", parent.display()))?;
        }
        crate::utils::write_atomic(&path, b"dirty")
            .with_context(|| format!("Failed to write {}", path.display()))
    }

    fn clear_run_index_dirty_locked(&self, automation_id: &str) -> Result<()> {
        let path = self.run_index_dirty_path(automation_id)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("Failed to remove {}", path.display()));
            }
        }
        self.forced_dirty_indexes
            .lock()
            .map_err(|_| anyhow::anyhow!("Automation dirty-index lock is poisoned"))?
            .remove(automation_id);
        Ok(())
    }

    fn run_index_is_dirty_locked(&self, automation_id: &str) -> Result<bool> {
        if self
            .forced_dirty_indexes
            .lock()
            .map_err(|_| anyhow::anyhow!("Automation dirty-index lock is poisoned"))?
            .contains(automation_id)
        {
            return Ok(true);
        }
        Ok(self.run_index_dirty_path(automation_id)?.exists())
    }

    fn reconciliation_runs(
        &self,
        automation_id: &str,
    ) -> Result<(Vec<AutomationRunRecord>, Option<DateTime<Utc>>)> {
        let _gate = self
            .index_gate
            .lock()
            .map_err(|_| anyhow::anyhow!("Automation run index lock is poisoned"))?;
        let index = self.load_or_rebuild_run_index_locked(automation_id)?;
        match self.reconciliation_runs_from_index_locked(automation_id, &index) {
            Ok(snapshot) => Ok(snapshot),
            Err(first_err) => {
                let rebuilt = self
                    .rebuild_run_index_locked(automation_id)
                    .with_context(|| format!("Run index active snapshot was stale: {first_err}"))?;
                self.reconciliation_runs_from_index_locked(automation_id, &rebuilt)
                    .with_context(|| format!("Run index remained stale after rebuild: {first_err}"))
            }
        }
    }

    fn reconciliation_runs_from_index_locked(
        &self,
        automation_id: &str,
        index: &AutomationRunIndex,
    ) -> Result<(Vec<AutomationRunRecord>, Option<DateTime<Utc>>)> {
        let mut runs = Vec::new();
        for (run_id, entry) in &index.entries {
            let needs_load = matches!(
                entry.status,
                AutomationRunStatus::Queued | AutomationRunStatus::Running
            ) || (is_terminal_run_status(entry.status)
                && entry.terminal_at.is_none());
            if !needs_load {
                continue;
            }
            let Some(run) = self.load_run(automation_id, run_id)? else {
                bail!("Run index references missing run '{run_id}'");
            };
            if run.created_at != entry.created_at || run.status != entry.status {
                bail!("Run index metadata for '{run_id}' is stale");
            }
            runs.push(run);
        }
        Ok((runs, index.latest_terminal_at))
    }

    fn save_pending_enqueue(&self, pending: &PendingEnqueueRecord) -> Result<()> {
        validate_pending_enqueue_record(pending, &pending.run.automation_id, &pending.run.id)?;
        let dir = self.pending_dir_for(&pending.run.automation_id)?;
        fs::create_dir_all(&dir).with_context(|| format!("Failed to create {}", dir.display()))?;
        write_json_atomic(
            &self.pending_path(&pending.run.automation_id, &pending.run.id)?,
            pending,
        )
    }

    fn load_pending_enqueue(
        &self,
        automation_id: &str,
        run_id: &str,
    ) -> Result<Option<PendingEnqueueRecord>> {
        let path = self.pending_path(automation_id, run_id)?;
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).with_context(|| format!("Failed to read {}", path.display()));
            }
        };
        let pending: PendingEnqueueRecord = serde_json::from_str(&raw)
            .with_context(|| format!("Failed to parse {}", path.display()))?;
        validate_pending_enqueue_record(&pending, automation_id, run_id)
            .with_context(|| format!("Invalid pending enqueue {}", path.display()))?;
        Ok(Some(pending))
    }

    fn list_pending_enqueues(&self, automation_id: &str) -> Result<Vec<PendingEnqueueRecord>> {
        Ok(self.scan_pending_enqueues(automation_id)?.0)
    }

    fn scan_pending_enqueues(
        &self,
        automation_id: &str,
    ) -> Result<(Vec<PendingEnqueueRecord>, bool)> {
        let dir = self.pending_dir_for(automation_id)?;
        if !dir.exists() {
            return Ok((Vec::new(), false));
        }
        let mut pending = Vec::new();
        let mut blocked = false;
        for entry in
            fs::read_dir(&dir).with_context(|| format!("Failed to read {}", dir.display()))?
        {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(err) => {
                    blocked = true;
                    tracing::warn!(automation_id, error = %err, "skipping unreadable pending entry");
                    continue;
                }
            };
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let loaded = (|| -> Result<PendingEnqueueRecord> {
                let file_stem = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .context("Pending enqueue filename must be valid UTF-8")?;
                let raw = fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?;
                let record: PendingEnqueueRecord = serde_json::from_str(&raw)
                    .with_context(|| format!("Failed to parse {}", path.display()))?;
                validate_pending_enqueue_record(&record, automation_id, file_stem)?;
                Ok(record)
            })();
            match loaded {
                Ok(record) => pending.push(record),
                Err(err) => {
                    blocked = true;
                    tracing::warn!(automation_id, path = %path.display(), error = %err, "leaving invalid pending enqueue in place");
                }
            }
        }
        pending.sort_by_key(|record| record.run.created_at);
        Ok((pending, blocked))
    }

    fn delete_pending_enqueue(&self, pending: &PendingEnqueueRecord) -> Result<()> {
        let path = self.pending_path(&pending.run.automation_id, &pending.run.id)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err)
                .with_context(|| format!("Failed to delete pending enqueue {}", path.display())),
        }
    }

    async fn enqueue_run_task(
        &self,
        automation: &AutomationRecord,
        run: &mut AutomationRunRecord,
        task_manager: &SharedTaskManager,
    ) -> Result<()> {
        self.enqueue_run_task_inner(automation, run, task_manager, None)
            .await
    }

    fn task_request_for(automation: &AutomationRecord) -> NewTaskRequest {
        NewTaskRequest {
            prompt: automation.prompt.clone(),
            model: automation.model.clone(),
            workspace: automation.cwds.first().cloned(),
            mode: Some(automation.task_mode()),
            allow_shell: Some(automation.task_allow_shell()),
            trust_mode: Some(automation.task_trust_mode()),
            auto_approve: Some(automation.task_auto_approve()),
        }
    }

    async fn enqueue_run_task_inner(
        &self,
        automation: &AutomationRecord,
        run: &mut AutomationRunRecord,
        task_manager: &SharedTaskManager,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let new_task = Self::task_request_for(automation);
        let task_result = match idempotency_key {
            Some(key) => {
                task_manager
                    .add_task_with_idempotency_key(new_task, key)
                    .await
            }
            None => task_manager.add_task(new_task).await,
        };

        match task_result {
            Ok(task) => {
                run.status = match task.status {
                    TaskStatus::Queued => AutomationRunStatus::Queued,
                    TaskStatus::Running => AutomationRunStatus::Running,
                    TaskStatus::Completed => AutomationRunStatus::Completed,
                    TaskStatus::Failed => AutomationRunStatus::Failed,
                    TaskStatus::Canceled => AutomationRunStatus::Canceled,
                };
                run.started_at = task.started_at;
                run.ended_at = task.ended_at;
                run.task_id = Some(task.id.clone());
                run.thread_id = task.thread_id.clone();
                run.turn_id = task.turn_id.clone();
                run.error = task.error.clone();
                Ok(())
            }
            Err(err) => {
                run.status = AutomationRunStatus::Failed;
                run.ended_at = Some(Utc::now());
                run.error = Some(format!("Failed to enqueue task: {err}"));
                Ok(())
            }
        }
    }

    async fn complete_pending_enqueue(
        &self,
        automation: &AutomationRecord,
        pending: &mut PendingEnqueueRecord,
        task_manager: &SharedTaskManager,
        fail_after_task_enqueue: bool,
    ) -> Result<()> {
        self.enqueue_run_task_inner(
            automation,
            &mut pending.run,
            task_manager,
            Some(&pending.slot_key),
        )
        .await?;

        if fail_after_task_enqueue && pending.run.task_id.is_some() {
            bail!("injected scheduler crash after durable task enqueue");
        }

        self.save_run(&pending.run)?;
        Ok(())
    }

    async fn recover_pending_enqueues(
        &self,
        task_manager: &SharedTaskManager,
        now: DateTime<Utc>,
    ) -> Result<(HashSet<String>, HashSet<String>)> {
        let mut recovered = HashSet::new();
        let mut blocked = HashSet::new();
        for mut automation in self.list_automations()? {
            let (pending_records, has_invalid) = match self.scan_pending_enqueues(&automation.id) {
                Ok(scan) => scan,
                Err(err) => {
                    tracing::warn!(automation_id = %automation.id, error = %err, "pending scan failed for automation");
                    blocked.insert(automation.id.clone());
                    continue;
                }
            };
            if has_invalid {
                blocked.insert(automation.id.clone());
                continue;
            }
            for mut pending in pending_records {
                self.complete_pending_enqueue(&automation, &mut pending, task_manager, false)
                    .await?;
                if matches!(pending.kind, PendingEnqueueKind::Scheduled)
                    && matches!(automation.status, AutomationStatus::Active)
                {
                    let schedule = AutomationSchedule::parse_rrule(&automation.rrule)?;
                    automation.next_run_at =
                        Some(schedule.next_due_after(pending.run.scheduled_for, now)?);
                    if matches!(
                        pending.run.status,
                        AutomationRunStatus::Completed
                            | AutomationRunStatus::Failed
                            | AutomationRunStatus::Canceled
                    ) {
                        advance_last_run_at(&mut automation, pending.run.ended_at.or(Some(now)));
                    }
                    automation.updated_at = now;
                    self.save_automation(&automation)?;
                    recovered.insert(automation.id.clone());
                } else if matches!(pending.kind, PendingEnqueueKind::Manual) {
                    automation.updated_at = now;
                    if matches!(
                        pending.run.status,
                        AutomationRunStatus::Completed
                            | AutomationRunStatus::Failed
                            | AutomationRunStatus::Canceled
                    ) {
                        advance_last_run_at(&mut automation, pending.run.ended_at.or(Some(now)));
                    }
                    self.save_automation(&automation)?;
                }
                self.delete_pending_enqueue(&pending)?;
            }
        }
        Ok((recovered, blocked))
    }

    pub async fn run_now(
        &self,
        automation_id: &str,
        task_manager: &SharedTaskManager,
    ) -> Result<AutomationRunRecord> {
        self.run_now_idempotent(automation_id, &Uuid::new_v4().to_string(), task_manager)
            .await
    }

    pub async fn run_now_idempotent(
        &self,
        automation_id: &str,
        invocation_id: &str,
        task_manager: &SharedTaskManager,
    ) -> Result<AutomationRunRecord> {
        let mut automation = self.get_automation(automation_id)?;
        let now = Utc::now();
        let new_pending = PendingEnqueueRecord::for_manual(&automation.id, invocation_id, now)?;

        if let Some(run) = self.load_run(&automation.id, &new_pending.run.id)? {
            if let Some(pending) = self.load_pending_enqueue(&automation.id, &new_pending.run.id)? {
                automation.updated_at = now;
                if matches!(
                    run.status,
                    AutomationRunStatus::Completed
                        | AutomationRunStatus::Failed
                        | AutomationRunStatus::Canceled
                ) {
                    advance_last_run_at(&mut automation, run.ended_at.or(Some(now)));
                }
                self.save_automation(&automation)?;
                self.delete_pending_enqueue(&pending)?;
            }
            return Ok(run);
        }

        let mut pending = match self.load_pending_enqueue(&automation.id, &new_pending.run.id)? {
            Some(pending) => pending,
            None => {
                self.save_pending_enqueue(&new_pending)?;
                new_pending
            }
        };

        self.complete_pending_enqueue(&automation, &mut pending, task_manager, false)
            .await?;

        automation.updated_at = Utc::now();
        if matches!(
            pending.run.status,
            AutomationRunStatus::Completed
                | AutomationRunStatus::Failed
                | AutomationRunStatus::Canceled
        ) {
            advance_last_run_at(&mut automation, pending.run.ended_at.or(Some(Utc::now())));
        }
        self.save_automation(&automation)?;
        self.delete_pending_enqueue(&pending)?;

        Ok(pending.run)
    }

    pub async fn scheduler_tick(&self, task_manager: &SharedTaskManager) -> Result<()> {
        self.scheduler_tick_inner(task_manager, false, Utc::now())
            .await
    }

    #[cfg(test)]
    async fn scheduler_tick_at(
        &self,
        task_manager: &SharedTaskManager,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.scheduler_tick_inner(task_manager, false, now).await
    }

    #[cfg(test)]
    async fn scheduler_tick_at_with_failure_after_task_enqueue(
        &self,
        task_manager: &SharedTaskManager,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.scheduler_tick_inner(task_manager, true, now).await
    }

    async fn scheduler_tick_inner(
        &self,
        task_manager: &SharedTaskManager,
        fail_after_task_enqueue: bool,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let (recovered, blocked) = self.recover_pending_enqueues(task_manager, now).await?;
        let mut automations = self.list_automations()?;

        for automation in &mut automations {
            if recovered.contains(&automation.id) || blocked.contains(&automation.id) {
                continue;
            }
            if !matches!(automation.status, AutomationStatus::Active) {
                continue;
            }

            let schedule = AutomationSchedule::parse_rrule(&automation.rrule)?;
            if automation.next_run_at.is_none() {
                automation.next_run_at = Some(schedule.next_after(now)?);
                automation.updated_at = now;
                self.save_automation(automation)?;
                continue;
            }

            let persisted_due_at = automation.next_run_at.expect("checked above");
            let first_due_at = schedule.normalize_due_cursor(persisted_due_at);
            if first_due_at != persisted_due_at {
                automation.next_run_at = Some(first_due_at);
                automation.updated_at = now;
                self.save_automation(automation)?;
            }
            if first_due_at > now {
                continue;
            }
            let due_at = schedule.latest_due_at_or_before(first_due_at, now)?;

            // Scheduled run IDs are deterministic, so idempotency never needs a history scan.
            let slot_run_id = format!("slot_{}", due_at.timestamp_micros());
            let existing_for_slot = match self.load_run(&automation.id, &slot_run_id) {
                Ok(Some(run)) if run.scheduled_for == due_at => true,
                Ok(Some(_)) => {
                    tracing::warn!(automation_id = %automation.id, run_id = %slot_run_id, "scheduled slot authority has mismatched timestamp; refusing duplicate enqueue");
                    continue;
                }
                Ok(None) => false,
                Err(err) => {
                    tracing::warn!(automation_id = %automation.id, run_id = %slot_run_id, error = %err, "scheduled slot authority is invalid; refusing duplicate enqueue");
                    continue;
                }
            };

            if existing_for_slot {
                automation.next_run_at = Some(schedule.next_after(due_at)?);
                automation.updated_at = now;
                self.save_automation(automation)?;
                continue;
            }

            let mut pending = PendingEnqueueRecord::for_slot(&automation.id, due_at, now);
            self.save_pending_enqueue(&pending)?;
            self.complete_pending_enqueue(
                automation,
                &mut pending,
                task_manager,
                fail_after_task_enqueue,
            )
            .await?;

            automation.updated_at = now;
            automation.next_run_at = Some(schedule.next_after(due_at)?);
            self.save_automation(automation)?;
            self.delete_pending_enqueue(&pending)?;
        }

        Ok(())
    }

    pub async fn reconcile_run_statuses(&self, task_manager: &SharedTaskManager) -> Result<()> {
        let automations = self.list_automations()?;
        for automation in automations {
            let observed_at = Utc::now();
            let (runs, mut latest_terminal_at) = match self.reconciliation_runs(&automation.id) {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    tracing::warn!(automation_id = %automation.id, error = %err, "skipping run reconciliation for invalid owner index");
                    continue;
                }
            };
            for mut run in runs {
                if matches!(
                    run.status,
                    AutomationRunStatus::Completed
                        | AutomationRunStatus::Failed
                        | AutomationRunStatus::Canceled
                ) {
                    let terminal_at = match run.ended_at {
                        Some(ended_at) => ended_at,
                        None => {
                            run.ended_at = Some(observed_at);
                            self.save_run(&run)?;
                            observed_at
                        }
                    };
                    latest_terminal_at = Some(
                        latest_terminal_at.map_or(terminal_at, |current: DateTime<Utc>| {
                            current.max(terminal_at)
                        }),
                    );
                    continue;
                }
                if !matches!(
                    run.status,
                    AutomationRunStatus::Queued | AutomationRunStatus::Running
                ) {
                    continue;
                }
                let Some(task_id) = run.task_id.clone() else {
                    continue;
                };
                let task = match task_manager.get_task(&task_id).await {
                    Ok(task) => task,
                    Err(_) => continue,
                };

                let mut changed = false;
                if run.thread_id != task.thread_id || run.turn_id != task.turn_id {
                    run.thread_id = task.thread_id.clone();
                    run.turn_id = task.turn_id.clone();
                    changed = true;
                }
                match task.status {
                    TaskStatus::Queued => {
                        if !matches!(run.status, AutomationRunStatus::Queued) {
                            run.status = AutomationRunStatus::Queued;
                            changed = true;
                        }
                    }
                    TaskStatus::Running => {
                        if !matches!(run.status, AutomationRunStatus::Running) {
                            run.status = AutomationRunStatus::Running;
                            changed = true;
                        }
                        if run.started_at.is_none() {
                            run.started_at = Some(task.started_at.unwrap_or(observed_at));
                            changed = true;
                        }
                    }
                    TaskStatus::Completed => {
                        run.status = AutomationRunStatus::Completed;
                        run.started_at = run.started_at.or(task.started_at);
                        run.ended_at = task.ended_at.or(Some(observed_at));
                        run.error = None;
                        changed = true;
                    }
                    TaskStatus::Failed => {
                        run.status = AutomationRunStatus::Failed;
                        run.started_at = run.started_at.or(task.started_at);
                        run.ended_at = task.ended_at.or(Some(observed_at));
                        run.error = task.error.clone();
                        changed = true;
                    }
                    TaskStatus::Canceled => {
                        run.status = AutomationRunStatus::Canceled;
                        run.started_at = run.started_at.or(task.started_at);
                        run.ended_at = task.ended_at.or(Some(observed_at));
                        changed = true;
                    }
                }

                if changed {
                    self.save_run(&run)?;
                }
                if matches!(
                    run.status,
                    AutomationRunStatus::Completed
                        | AutomationRunStatus::Failed
                        | AutomationRunStatus::Canceled
                ) {
                    let terminal_at = run.ended_at.unwrap_or(observed_at);
                    latest_terminal_at = Some(
                        latest_terminal_at.map_or(terminal_at, |current: DateTime<Utc>| {
                            current.max(terminal_at)
                        }),
                    );
                }
            }
            if let Some(terminal_at) = latest_terminal_at {
                let mut updated_automation = self.get_automation(&automation.id)?;
                let previous = updated_automation.last_run_at;
                advance_last_run_at(&mut updated_automation, Some(terminal_at));
                if updated_automation.last_run_at != previous {
                    updated_automation.updated_at = observed_at;
                    self.save_automation(&updated_automation)?;
                }
            }
        }

        Ok(())
    }
}

fn is_terminal_run_status(status: AutomationRunStatus) -> bool {
    matches!(
        status,
        AutomationRunStatus::Completed
            | AutomationRunStatus::Failed
            | AutomationRunStatus::Canceled
    )
}

fn validate_run_record(
    run: &AutomationRunRecord,
    expected_automation_id: &str,
    expected_run_id: &str,
) -> Result<()> {
    if run.schema_version > CURRENT_RUN_SCHEMA_VERSION {
        bail!(
            "Automation run schema v{} is newer than supported v{}",
            run.schema_version,
            CURRENT_RUN_SCHEMA_VERSION
        );
    }
    ensure_safe_storage_id("automation id", expected_automation_id)?;
    ensure_safe_storage_id("run filename", expected_run_id)?;
    ensure_safe_storage_id("run automation id", &run.automation_id)?;
    ensure_safe_storage_id("run id", &run.id)?;
    if run.automation_id != expected_automation_id || run.id != expected_run_id {
        bail!("Automation run has mismatched identity");
    }
    Ok(())
}

fn read_run_json(
    path: &Path,
    raw: &str,
    expected_automation_id: &str,
    expected_run_id: &str,
) -> Result<AutomationRunRecord> {
    let run: AutomationRunRecord =
        serde_json::from_str(raw).with_context(|| format!("Failed to parse {}", path.display()))?;
    validate_run_record(&run, expected_automation_id, expected_run_id)
        .with_context(|| format!("Invalid automation run {}", path.display()))?;
    Ok(run)
}

fn validate_run_index(index: &AutomationRunIndex, expected_automation_id: &str) -> Result<()> {
    if index.schema_version != CURRENT_RUN_INDEX_SCHEMA_VERSION {
        bail!(
            "Unsupported run index schema v{} (expected v{})",
            index.schema_version,
            CURRENT_RUN_INDEX_SCHEMA_VERSION
        );
    }
    ensure_safe_storage_id("automation id", expected_automation_id)?;
    if index.automation_id != expected_automation_id {
        bail!("Run index belongs to a different automation");
    }
    for (run_id, entry) in &index.entries {
        ensure_safe_storage_id("indexed run id", run_id)?;
        if !is_terminal_run_status(entry.status) && entry.terminal_at.is_some() {
            bail!("Active run '{run_id}' has terminal metadata in index");
        }
    }
    let expected_latest = index
        .entries
        .values()
        .filter_map(|entry| {
            is_terminal_run_status(entry.status)
                .then_some(entry.terminal_at)
                .flatten()
        })
        .max();
    if index.latest_terminal_at != expected_latest {
        bail!("Run index latest terminal metadata is inconsistent");
    }
    Ok(())
}

fn refresh_latest_terminal_at(index: &mut AutomationRunIndex) {
    index.latest_terminal_at = index
        .entries
        .values()
        .filter_map(|entry| {
            is_terminal_run_status(entry.status)
                .then_some(entry.terminal_at)
                .flatten()
        })
        .max();
}

fn advance_last_run_at(automation: &mut AutomationRecord, candidate: Option<DateTime<Utc>>) {
    let Some(candidate) = candidate else {
        return;
    };
    automation.last_run_at = Some(
        automation
            .last_run_at
            .map_or(candidate, |current| current.max(candidate)),
    );
}

fn ensure_safe_storage_id(kind: &str, value: &str) -> Result<()> {
    let mut components = Path::new(value).components();
    let Some(component) = components.next() else {
        bail!("{kind} must not be empty");
    };
    if components.next().is_some() || !matches!(component, std::path::Component::Normal(_)) {
        bail!("{kind} must be a single path component");
    }
    Ok(())
}

fn validate_name_and_prompt(name: &str, prompt: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("Automation name is required");
    }
    if prompt.trim().is_empty() {
        bail!("Automation prompt is required");
    }
    Ok(())
}

fn validate_automation_record(record: &AutomationRecord, expected_id: &str) -> Result<()> {
    ensure_safe_storage_id("automation filename", expected_id)?;
    ensure_safe_storage_id("automation id", &record.id)?;
    if record.id != expected_id {
        bail!(
            "Automation id '{}' does not match file stem '{}'",
            record.id,
            expected_id
        );
    }
    validate_name_and_prompt(&record.name, &record.prompt)?;
    AutomationSchedule::parse_rrule(&record.rrule)?;
    validate_persisted_mode(record.mode.as_deref())?;
    Ok(())
}

fn validate_pending_enqueue_record(
    record: &PendingEnqueueRecord,
    expected_automation_id: &str,
    expected_run_id: &str,
) -> Result<()> {
    if record.schema_version > CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION {
        bail!(
            "Pending enqueue schema v{} is newer than supported v{}",
            record.schema_version,
            CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION
        );
    }
    if record.run.schema_version > CURRENT_RUN_SCHEMA_VERSION {
        bail!(
            "Automation run schema v{} is newer than supported v{}",
            record.run.schema_version,
            CURRENT_RUN_SCHEMA_VERSION
        );
    }
    ensure_safe_storage_id("automation id", expected_automation_id)?;
    ensure_safe_storage_id("pending enqueue filename", expected_run_id)?;
    ensure_safe_storage_id("run automation id", &record.run.automation_id)?;
    ensure_safe_storage_id("run id", &record.run.id)?;
    if record.run.automation_id != expected_automation_id {
        bail!(
            "Pending enqueue belongs to automation {}, expected {}",
            record.run.automation_id,
            expected_automation_id
        );
    }
    if record.run.id != expected_run_id {
        bail!(
            "Pending enqueue run id '{}' does not match file stem '{}'",
            record.run.id,
            expected_run_id
        );
    }

    let (expected_id, expected_slot_key) = match record.kind {
        PendingEnqueueKind::Scheduled => {
            let slot_timestamp = record.run.scheduled_for.timestamp_micros();
            (
                format!("slot_{slot_timestamp}"),
                format!("automation:{expected_automation_id}:slot:{slot_timestamp}"),
            )
        }
        PendingEnqueueKind::Manual => {
            let invocation_id = record
                .run
                .id
                .strip_prefix("manual_")
                .context("Manual pending enqueue run id must start with 'manual_'")?;
            let invocation_id = Uuid::parse_str(invocation_id)
                .context("Manual pending enqueue run id must contain a UUID")?
                .to_string();
            (
                format!("manual_{invocation_id}"),
                format!("automation:{expected_automation_id}:manual:{invocation_id}"),
            )
        }
    };
    if record.run.id != expected_id {
        bail!(
            "Pending enqueue run id '{}' is inconsistent with its {:?} kind",
            record.run.id,
            record.kind
        );
    }
    if record.slot_key != expected_slot_key {
        bail!(
            "Pending enqueue slot key '{}' does not match expected '{}'",
            record.slot_key,
            expected_slot_key
        );
    }
    Ok(())
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_optional_mode(value: Option<String>) -> Result<Option<String>> {
    let normalized = normalize_optional_string(value).map(|mode| mode.to_ascii_lowercase());
    validate_persisted_mode(normalized.as_deref())?;
    Ok(normalized)
}

fn normalize_persisted_mode(record: &mut AutomationRecord) -> Result<bool> {
    let Some(mode) = record.mode.as_deref() else {
        return Ok(false);
    };
    let canonical = match mode.trim().to_ascii_lowercase().as_str() {
        "agent" | "1" => "agent",
        "plan" | "2" => "plan",
        "yolo" | "3" => "yolo",
        _ => {
            bail!("Invalid automation mode '{mode}'. Expected one of: agent, plan, yolo");
        }
    };
    if mode == canonical {
        return Ok(false);
    }
    record.mode = Some(canonical.to_string());
    Ok(true)
}

fn validate_persisted_mode(mode: Option<&str>) -> Result<()> {
    if let Some(mode) = mode
        && !matches!(mode, "agent" | "plan" | "yolo")
    {
        bail!("Invalid automation mode '{mode}'. Expected one of: agent, plan, yolo");
    }
    Ok(())
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(value)?;
    crate::utils::write_atomic(path, content.as_bytes())
        .with_context(|| format!("Failed to atomically write {}", path.display()))
}

pub fn default_automations_dir() -> PathBuf {
    // Most-specific override: an explicit automations dir.
    if let Ok(path) = std::env::var("DEEPSEEK_AUTOMATIONS_DIR") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    // $CODEWHALE_HOME is a hard override of the base data directory
    // (docs/CONFIGURATION.md): when SET, automations live under it and we do
    // NOT fall back to the legacy ~/.deepseek path — silent fallback would
    // defeat the isolation the override promises. Check the env var directly
    // (not codewhale_home()'s Ok/Err, which succeeds for the default home too).
    if let Some(home) = std::env::var_os("CODEWHALE_HOME").filter(|value| !value.is_empty()) {
        return PathBuf::from(home).join("automations");
    }
    dirs::home_dir()
        .map(|home| {
            let primary = home.join(".codewhale").join("automations");
            let legacy = home.join(".deepseek").join("automations");
            if primary.exists() || !legacy.exists() {
                return primary;
            }
            legacy
        })
        .unwrap_or_else(|| PathBuf::from(".codewhale").join("automations"))
}

pub type SharedAutomationManager = Arc<Mutex<AutomationManager>>;

#[derive(Debug, Clone)]
pub struct AutomationSchedulerConfig {
    pub tick_interval_secs: u64,
}

impl Default for AutomationSchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval_secs: 15,
        }
    }
}

pub fn spawn_scheduler(
    automations: SharedAutomationManager,
    task_manager: SharedTaskManager,
    cancel: CancellationToken,
    config: AutomationSchedulerConfig,
) -> tokio::task::JoinHandle<()> {
    spawn_supervised(
        "automation-scheduler",
        std::panic::Location::caller(),
        async move {
            let interval = config.tick_interval_secs.max(5);
            loop {
                if cancel.is_cancelled() {
                    break;
                }

                {
                    let manager = automations.lock().await;
                    if let Err(err) = manager.scheduler_tick(&task_manager).await {
                        tracing::warn!("automation scheduler tick failed: {err}");
                    }
                    if let Err(err) = manager.reconcile_run_statuses(&task_manager).await {
                        tracing::warn!("automation reconcile failed: {err}");
                    }
                }

                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = sleep(std::time::Duration::from_secs(interval)) => {}
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_manager::{
        ExecutionTask, TaskExecutionEvent, TaskExecutionReporter, TaskExecutionResult,
        TaskExecutor, TaskManager, TaskManagerConfig, wait_for_terminal_state,
    };
    use async_trait::async_trait;

    struct AutomationNoopExecutor;

    struct LinkedBlockingExecutor;

    struct ProtectNamedRunGuard {
        protected_run_id: String,
        fail: bool,
    }

    impl AutomationRunRetentionGuard for ProtectNamedRunGuard {
        fn retain_terminal_run(&self, run: &AutomationRunRecord) -> Result<bool> {
            if self.fail {
                bail!("injected retention guard failure");
            }
            Ok(run.id == self.protected_run_id)
        }
    }

    #[async_trait]
    impl TaskExecutor for AutomationNoopExecutor {
        async fn execute(
            &self,
            _task: ExecutionTask,
            _reporter: TaskExecutionReporter,
            _cancel: CancellationToken,
        ) -> TaskExecutionResult {
            TaskExecutionResult {
                status: TaskStatus::Completed,
                result_text: Some("done".to_string()),
                error: None,
            }
        }
    }

    #[async_trait]
    impl TaskExecutor for LinkedBlockingExecutor {
        async fn execute(
            &self,
            task: ExecutionTask,
            reporter: TaskExecutionReporter,
            cancel: CancellationToken,
        ) -> TaskExecutionResult {
            reporter
                .report(TaskExecutionEvent::ThreadCreated {
                    thread_id: format!("sched-{}", task.id()),
                })
                .await
                .expect("persist ThreadCreated");
            reporter
                .report(TaskExecutionEvent::ThreadLinked {
                    thread_id: format!("sched-{}", task.id()),
                    turn_id: "turn-running".to_string(),
                })
                .await
                .expect("persist ThreadLinked");
            cancel.cancelled().await;
            TaskExecutionResult {
                status: TaskStatus::Canceled,
                result_text: None,
                error: None,
            }
        }
    }

    fn automation_task_config(root: PathBuf) -> TaskManagerConfig {
        TaskManagerConfig {
            data_dir: root,
            worker_count: 1,
            default_workspace: PathBuf::from("."),
            default_model: "deepseek-v4-flash".to_string(),
            default_mode: "plan".to_string(),
            allow_shell: true,
            trust_mode: true,
            max_subagents: 2,
        }
    }

    fn automation_record_with_settings(
        mode: Option<&str>,
        allow_shell: Option<bool>,
        trust_mode: Option<bool>,
        auto_approve: Option<bool>,
    ) -> AutomationRecord {
        let now = Utc::now();
        AutomationRecord {
            schema_version: CURRENT_AUTOMATION_SCHEMA_VERSION,
            id: Uuid::new_v4().to_string(),
            name: "Test automation".to_string(),
            prompt: "Run the automation".to_string(),
            rrule: "FREQ=HOURLY;INTERVAL=1".to_string(),
            cwds: Vec::new(),
            model: None,
            mode: mode.map(ToString::to_string),
            allow_shell,
            trust_mode,
            auto_approve,
            status: AutomationStatus::Active,
            created_at: now,
            updated_at: now,
            next_run_at: None,
            last_run_at: None,
        }
    }

    fn queued_run_for(automation: &AutomationRecord) -> AutomationRunRecord {
        let now = Utc::now();
        AutomationRunRecord {
            schema_version: CURRENT_RUN_SCHEMA_VERSION,
            id: Uuid::new_v4().to_string(),
            automation_id: automation.id.clone(),
            scheduled_for: now,
            status: AutomationRunStatus::Queued,
            created_at: now,
            started_at: None,
            ended_at: None,
            task_id: None,
            thread_id: None,
            turn_id: None,
            error: None,
        }
    }

    fn terminal_run_for(
        automation: &AutomationRecord,
        id: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> AutomationRunRecord {
        AutomationRunRecord {
            schema_version: CURRENT_RUN_SCHEMA_VERSION,
            id: id.into(),
            automation_id: automation.id.clone(),
            scheduled_for: created_at,
            status: AutomationRunStatus::Completed,
            created_at,
            started_at: Some(created_at),
            ended_at: Some(created_at),
            task_id: None,
            thread_id: None,
            turn_id: None,
            error: None,
        }
    }

    #[test]
    fn parses_hourly_rrule() {
        let parsed =
            AutomationSchedule::parse_rrule("FREQ=HOURLY;INTERVAL=2;BYDAY=MO,TU").expect("parse");
        match parsed {
            AutomationSchedule::Hourly {
                interval_hours,
                byday,
            } => {
                assert_eq!(interval_hours, 2);
                assert_eq!(byday.expect("byday").len(), 2);
            }
            _ => panic!("expected hourly"),
        }
    }

    #[test]
    fn forkguard_parses_minutely_rrule() {
        let parsed = AutomationSchedule::parse_rrule("FREQ=MINUTELY;INTERVAL=10").expect("parse");
        match parsed {
            AutomationSchedule::Minutely { interval_minutes } => {
                assert_eq!(interval_minutes, 10);
            }
            _ => panic!("expected minutely"),
        }
    }

    #[test]
    fn minutely_next_after_rounds_to_minute_boundary() {
        let schedule = AutomationSchedule::parse_rrule("FREQ=MINUTELY;INTERVAL=10").expect("parse");
        let after = DateTime::parse_from_rfc3339("2026-07-09T09:17:23.456Z")
            .expect("time")
            .with_timezone(&Utc);

        let next = schedule.next_after(after).expect("next");

        assert_eq!(next.second(), 0);
        assert_eq!(next.nanosecond(), 0);
        assert_eq!((next - after).num_minutes(), 9);
    }

    #[test]
    fn minutely_fast_forward_normalizes_legacy_cursor_at_end_of_minute() {
        let schedule = AutomationSchedule::parse_rrule("FREQ=MINUTELY;INTERVAL=1").expect("parse");
        let first_due = DateTime::parse_from_rfc3339("2026-07-09T09:17:59.900Z")
            .expect("first due")
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-07-09T09:27:59.900Z")
            .expect("now")
            .with_timezone(&Utc);

        let latest = schedule
            .latest_due_at_or_before(first_due, now)
            .expect("fast forward");
        let next = schedule.next_after(latest).expect("next");

        assert_eq!(latest.to_rfc3339(), "2026-07-09T09:27:00+00:00");
        assert_eq!(next.to_rfc3339(), "2026-07-09T09:28:00+00:00");
        assert!(next > now);
    }

    #[test]
    fn rejects_zero_minutely_interval() {
        let err =
            AutomationSchedule::parse_rrule("FREQ=MINUTELY;INTERVAL=0").expect_err("should fail");
        assert!(
            err.to_string()
                .contains("INTERVAL must be >= 1 for MINUTELY schedules")
        );
    }

    #[test]
    fn rejects_unsupported_minutely_fields() {
        let err = AutomationSchedule::parse_rrule("FREQ=MINUTELY;INTERVAL=10;BYDAY=MO")
            .expect_err("should fail");
        assert!(err.to_string().contains("Unsupported RRULE field"));
    }

    #[test]
    fn fast_forward_finds_latest_hourly_byday_slot() {
        let schedule =
            AutomationSchedule::parse_rrule("FREQ=HOURLY;INTERVAL=1;BYDAY=MO,TU").expect("parse");
        let first_due = DateTime::parse_from_rfc3339("2026-07-06T01:00:00Z")
            .expect("first due")
            .with_timezone(&Utc);
        let now = first_due + Duration::days(4);

        let latest = schedule
            .latest_due_at_or_before(first_due, now)
            .expect("fast forward");

        assert!(latest <= now);
        assert!(matches!(
            latest.with_timezone(&Local).weekday(),
            Weekday::Mon | Weekday::Tue
        ));
        assert!(schedule.next_after(latest).expect("next") > now);
    }

    #[test]
    fn fast_forward_finds_latest_weekly_slot() {
        let schedule =
            AutomationSchedule::parse_rrule("FREQ=WEEKLY;BYDAY=MO,WE;BYHOUR=9;BYMINUTE=30")
                .expect("parse");
        let anchor = DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
            .expect("anchor")
            .with_timezone(&Utc);
        let first_due = schedule.next_after(anchor).expect("first due");
        let now = first_due + Duration::days(30);

        let latest = schedule
            .latest_due_at_or_before(first_due, now)
            .expect("fast forward");

        assert!(latest <= now);
        assert!(matches!(
            latest.with_timezone(&Local).weekday(),
            Weekday::Mon | Weekday::Wed
        ));
        assert!(schedule.next_after(latest).expect("next") > now);
    }

    #[test]
    fn parses_weekly_rrule() {
        let parsed =
            AutomationSchedule::parse_rrule("FREQ=WEEKLY;BYDAY=MO,WE;BYHOUR=9;BYMINUTE=30")
                .expect("parse");
        match parsed {
            AutomationSchedule::Weekly {
                byday,
                byhour,
                byminute,
            } => {
                assert_eq!(byday.len(), 2);
                assert_eq!(byhour, 9);
                assert_eq!(byminute, 30);
            }
            _ => panic!("expected weekly"),
        }
    }

    #[test]
    fn rejects_invalid_rrule_fields() {
        let err =
            AutomationSchedule::parse_rrule("FREQ=WEEKLY;BYSECOND=5").expect_err("should fail");
        assert!(err.to_string().contains("Unsupported RRULE field"));
    }

    #[test]
    fn deletes_automation_and_runs() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let manager = AutomationManager::open(tempdir.path().to_path_buf()).expect("manager");

        let created = manager
            .create_automation(CreateAutomationRequest {
                name: "Delete me".to_string(),
                prompt: "prompt".to_string(),
                rrule: "FREQ=HOURLY;INTERVAL=1".to_string(),
                cwds: Vec::new(),
                model: None,
                mode: None,
                allow_shell: None,
                trust_mode: None,
                auto_approve: None,
                status: Some(AutomationStatus::Active),
            })
            .expect("create");

        let run = AutomationRunRecord {
            schema_version: CURRENT_RUN_SCHEMA_VERSION,
            id: Uuid::new_v4().to_string(),
            automation_id: created.id.clone(),
            scheduled_for: Utc::now(),
            status: AutomationRunStatus::Queued,
            created_at: Utc::now(),
            started_at: None,
            ended_at: None,
            task_id: None,
            thread_id: None,
            turn_id: None,
            error: None,
        };
        manager.save_run(&run).expect("save run");
        assert!(
            manager
                .runs_dir_for(&created.id)
                .expect("runs dir")
                .exists()
        );

        manager
            .delete_automation(&created.id)
            .expect("delete automation");

        assert!(manager.get_automation(&created.id).is_err());
        assert!(
            !manager
                .runs_dir_for(&created.id)
                .expect("runs dir")
                .exists()
        );
    }

    #[test]
    fn automation_storage_rejects_traversal_ids() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let manager = AutomationManager::open(tempdir.path().join("root")).expect("manager");
        let escaped_file = tempdir.path().join("escape.json");
        let escaped_runs = tempdir.path().join("escape-runs");

        let err = manager
            .get_automation("../escape")
            .expect_err("traversal automation ids must be rejected");
        assert!(err.to_string().contains("single path component"));
        assert!(!escaped_file.exists());

        let err = manager
            .list_runs("../escape-runs", None)
            .expect_err("traversal run dirs must be rejected");
        assert!(err.to_string().contains("single path component"));
        assert!(!escaped_runs.exists());

        let run = AutomationRunRecord {
            schema_version: CURRENT_RUN_SCHEMA_VERSION,
            id: "../escape-run".to_string(),
            automation_id: Uuid::new_v4().to_string(),
            scheduled_for: Utc::now(),
            status: AutomationRunStatus::Queued,
            created_at: Utc::now(),
            started_at: None,
            ended_at: None,
            task_id: None,
            thread_id: None,
            turn_id: None,
            error: None,
        };
        let err = manager
            .save_run(&run)
            .expect_err("traversal run ids must be rejected");
        assert!(err.to_string().contains("single path component"));
        assert!(!tempdir.path().join("escape-run.json").exists());
    }

    #[test]
    fn automation_task_settings_default_for_legacy_records() {
        let now = Utc::now().to_rfc3339();
        let record: AutomationRecord = serde_json::from_value(serde_json::json!({
            "schema_version": CURRENT_AUTOMATION_SCHEMA_VERSION,
            "id": Uuid::new_v4().to_string(),
            "name": "Legacy automation",
            "prompt": "Run legacy automation",
            "rrule": "FREQ=HOURLY;INTERVAL=1",
            "cwds": [],
            "status": "active",
            "created_at": now,
            "updated_at": now
        }))
        .expect("legacy automation record should deserialize");

        assert_eq!(record.mode, None);
        assert_eq!(record.model, None);
        assert_eq!(record.task_mode(), "agent");
        assert!(!record.task_allow_shell());
        assert!(!record.task_trust_mode());
        assert!(!record.task_auto_approve());
    }

    #[test]
    fn legacy_modes_are_normalized_without_poisoning_the_list() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let aliases = [(" Agent ", "agent"), ("2", "plan"), ("3", "yolo")];
        let mut expected = BTreeMap::new();

        for (stored, canonical) in aliases {
            let mut automation = automation_record_with_settings(Some(stored), None, None, None);
            automation.id = Uuid::new_v4().to_string();
            write_json_atomic(&manager.automation_path(&automation.id)?, &automation)?;
            expected.insert(automation.id, canonical.to_string());
        }

        let unknown = automation_record_with_settings(Some("legacy-custom"), None, None, None);
        manager
            .save_automation(&unknown)
            .expect_err("public saves must accept only canonical modes");
        write_json_atomic(&manager.automation_path(&unknown.id)?, &unknown)?;

        let listed = manager.list_automations()?;

        assert_eq!(listed.len(), expected.len());
        for automation in listed {
            let canonical = expected.get(&automation.id).expect("known test record");
            assert_eq!(automation.mode.as_deref(), Some(canonical.as_str()));

            let persisted: AutomationRecord = serde_json::from_str(&fs::read_to_string(
                manager.automation_path(&automation.id)?,
            )?)?;
            assert_eq!(persisted.mode.as_deref(), Some(canonical.as_str()));
        }
        let error = manager
            .get_automation(&unknown.id)
            .expect_err("targeted reads must report an unknown persisted mode");
        assert!(error.to_string().contains("Invalid automation mode"));
        Ok(())
    }

    #[test]
    fn invalid_automation_is_isolated_from_valid_records() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let invalid_path = manager.automation_path("broken")?;
        fs::write(&invalid_path, "{ definitely not valid json")?;

        let listed = manager.list_automations()?;

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, automation.id);
        let error = manager
            .get_automation("broken")
            .expect_err("targeted reads must preserve the concrete parse error");
        assert!(error.to_string().contains("Failed to parse automation"));
        assert!(
            invalid_path.exists(),
            "invalid records must remain inspectable"
        );
        Ok(())
    }

    #[tokio::test]
    async fn invalid_automation_semantics_do_not_block_healthy_scheduler() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let now = Utc::now();
        let mut healthy = automation_record_with_settings(None, None, None, None);
        healthy.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        healthy.next_run_at = Some(now - Duration::minutes(1));
        manager.save_automation(&healthy)?;

        let mut bad_rrule = automation_record_with_settings(None, None, None, None);
        bad_rrule.rrule = "FREQ=DAILY".to_string();
        write_json_atomic(&manager.automation_path(&bad_rrule.id)?, &bad_rrule)?;

        let mut mismatched = automation_record_with_settings(None, None, None, None);
        mismatched.id = Uuid::new_v4().to_string();
        write_json_atomic(&manager.automation_path("mismatched-file")?, &mismatched)?;

        let mut unsafe_id = automation_record_with_settings(None, None, None, None);
        unsafe_id.id = "../escape".to_string();
        write_json_atomic(&manager.automation_path("unsafe-record")?, &unsafe_id)?;

        let mut empty_name = automation_record_with_settings(None, None, None, None);
        empty_name.name.clear();
        write_json_atomic(&manager.automation_path(&empty_name.id)?, &empty_name)?;

        let mut empty_prompt = automation_record_with_settings(None, None, None, None);
        empty_prompt.prompt.clear();
        write_json_atomic(&manager.automation_path(&empty_prompt.id)?, &empty_prompt)?;

        manager.scheduler_tick(&task_manager).await?;

        let listed = manager.list_automations()?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, healthy.id);
        assert_eq!(manager.list_runs(&healthy.id, None)?.len(), 1);
        assert_eq!(task_manager.list_tasks(None).await.len(), 1);

        let rrule_error = manager
            .get_automation(&bad_rrule.id)
            .expect_err("targeted read must report invalid RRULE");
        assert!(rrule_error.to_string().contains("Unsupported RRULE FREQ"));
        let identity_error = manager
            .get_automation("mismatched-file")
            .expect_err("targeted read must report file/record identity mismatch");
        assert!(identity_error.to_string().contains("does not match"));

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn automation_enqueue_uses_default_and_explicit_task_settings() -> Result<()> {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let automation_manager =
            AutomationManager::open(tempdir.path().join("automations")).expect("manager");
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            std::sync::Arc::new(AutomationNoopExecutor),
        )
        .await?;

        let default_automation = automation_record_with_settings(None, None, None, None);
        let mut default_run = queued_run_for(&default_automation);
        automation_manager
            .enqueue_run_task(&default_automation, &mut default_run, &task_manager)
            .await?;
        let default_task = task_manager
            .get_task(default_run.task_id.as_deref().expect("task id"))
            .await?;
        assert_eq!(default_task.mode, "agent");
        assert!(!default_task.allow_shell);
        assert!(!default_task.trust_mode);
        assert!(!default_task.auto_approve);

        let mut explicit_automation =
            automation_record_with_settings(Some("plan"), Some(true), Some(true), Some(false));
        explicit_automation.model = Some("scheduled-model".to_string());
        let mut explicit_run = queued_run_for(&explicit_automation);
        automation_manager
            .enqueue_run_task(&explicit_automation, &mut explicit_run, &task_manager)
            .await?;
        let explicit_task = task_manager
            .get_task(explicit_run.task_id.as_deref().expect("task id"))
            .await?;
        assert_eq!(explicit_task.mode, "plan");
        assert_eq!(explicit_task.model, "scheduled-model");
        assert!(explicit_task.allow_shell);
        assert!(explicit_task.trust_mode);
        assert!(!explicit_task.auto_approve);

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn enqueue_run_reflects_queued_task_without_fake_start_time() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(LinkedBlockingExecutor),
        )
        .await?;
        let blocker = task_manager
            .add_task(NewTaskRequest {
                prompt: "occupy the only worker".to_string(),
                model: None,
                workspace: None,
                mode: Some("agent".to_string()),
                allow_shell: Some(false),
                trust_mode: Some(false),
                auto_approve: Some(false),
            })
            .await?;
        for _ in 0..100 {
            if task_manager.get_task(&blocker.id).await?.status == TaskStatus::Running {
                break;
            }
            sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            task_manager.get_task(&blocker.id).await?.status,
            TaskStatus::Running
        );

        let automation = automation_record_with_settings(None, None, None, None);
        let mut run = queued_run_for(&automation);
        manager
            .enqueue_run_task(&automation, &mut run, &task_manager)
            .await?;
        let queued_task = task_manager
            .get_task(run.task_id.as_deref().expect("queued task id"))
            .await?;

        assert_eq!(queued_task.status, TaskStatus::Queued);
        assert_eq!(run.status, AutomationRunStatus::Queued);
        assert_eq!(run.started_at, None);

        task_manager.cancel_task(&blocker.id).await?;
        task_manager
            .cancel_task(run.task_id.as_deref().expect("queued task id"))
            .await?;
        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_persists_runtime_link_while_task_is_still_running() -> Result<()> {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let automation_manager =
            AutomationManager::open(tempdir.path().join("automations")).expect("manager");
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            std::sync::Arc::new(LinkedBlockingExecutor),
        )
        .await?;

        let automation = automation_record_with_settings(None, None, None, None);
        automation_manager.save_automation(&automation)?;
        let mut run = queued_run_for(&automation);
        automation_manager
            .enqueue_run_task(&automation, &mut run, &task_manager)
            .await?;
        automation_manager.save_run(&run)?;

        let task_id = run.task_id.as_deref().expect("task id");
        for _ in 0..100 {
            if task_manager.get_task(task_id).await?.turn_id.is_some() {
                break;
            }
            sleep(std::time::Duration::from_millis(10)).await;
        }
        let running = task_manager.get_task(task_id).await?;
        assert_eq!(running.status, TaskStatus::Running);
        assert_eq!(running.turn_id.as_deref(), Some("turn-running"));

        automation_manager
            .reconcile_run_statuses(&task_manager)
            .await?;
        let persisted = automation_manager
            .list_runs(&automation.id, None)?
            .into_iter()
            .find(|candidate| candidate.id == run.id)
            .expect("run persisted");
        assert_eq!(persisted.thread_id, running.thread_id);
        assert_eq!(persisted.turn_id, running.turn_id);

        task_manager.cancel_task(task_id).await?;
        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn last_run_at_never_moves_backwards() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let mut automation = automation_record_with_settings(None, None, None, None);
        let existing_last_run_at = Utc::now() + Duration::hours(1);
        automation.last_run_at = Some(existing_last_run_at);
        manager.save_automation(&automation)?;

        let mut run = queued_run_for(&automation);
        manager
            .enqueue_run_task(&automation, &mut run, &task_manager)
            .await?;
        manager.save_run(&run)?;
        let task_id = run.task_id.as_deref().expect("task id");
        wait_for_terminal_state(&task_manager, task_id, std::time::Duration::from_secs(10)).await?;

        manager.reconcile_run_statuses(&task_manager).await?;

        assert_eq!(
            manager.get_automation(&automation.id)?.last_run_at,
            Some(existing_last_run_at)
        );
        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_repairs_last_run_at_from_unchanged_terminal_runs() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;

        let terminal_at = Utc::now() - Duration::minutes(1);
        let mut run = queued_run_for(&automation);
        run.status = AutomationRunStatus::Completed;
        run.ended_at = Some(terminal_at);
        manager.save_run(&run)?;

        manager.reconcile_run_statuses(&task_manager).await?;

        assert_eq!(
            manager.get_automation(&automation.id)?.last_run_at,
            Some(terminal_at)
        );
        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_stabilizes_legacy_terminal_run_without_ended_at() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;

        let mut run = queued_run_for(&automation);
        run.status = AutomationRunStatus::Completed;
        run.ended_at = None;
        manager.save_run(&run)?;

        manager.reconcile_run_statuses(&task_manager).await?;
        let first_automation = manager.get_automation(&automation.id)?;
        let first_last_run_at = first_automation
            .last_run_at
            .expect("first reconcile cursor");

        sleep(std::time::Duration::from_millis(5)).await;
        manager.reconcile_run_statuses(&task_manager).await?;

        let second_automation = manager.get_automation(&automation.id)?;
        let persisted_run = manager
            .list_runs(&automation.id, None)?
            .into_iter()
            .find(|candidate| candidate.id == run.id)
            .expect("legacy run remains persisted");
        assert_eq!(second_automation.last_run_at, Some(first_last_run_at));
        assert_eq!(second_automation.updated_at, first_automation.updated_at);
        assert_eq!(persisted_run.ended_at, Some(first_last_run_at));

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_fast_forwards_stale_slots_without_replaying_backlog() -> Result<()> {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let automation_manager =
            AutomationManager::open(tempdir.path().join("automations")).expect("manager");
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            std::sync::Arc::new(AutomationNoopExecutor),
        )
        .await?;

        let now = DateTime::parse_from_rfc3339("2026-07-09T09:27:59.900Z")
            .expect("now")
            .with_timezone(&Utc);
        let mut automation = automation_record_with_settings(None, None, None, None);
        automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        automation.next_run_at = Some(now - Duration::minutes(10));
        automation_manager.save_automation(&automation)?;

        automation_manager
            .scheduler_tick_at(&task_manager, now)
            .await?;
        let after_first_tick = automation_manager.get_automation(&automation.id)?;
        assert!(
            after_first_tick.next_run_at.expect("next run") > now,
            "the next slot must be in the future after one catch-up run"
        );
        let first_runs = automation_manager.list_runs(&automation.id, None)?;
        assert_eq!(first_runs.len(), 1);
        assert!(first_runs[0].scheduled_for > now - Duration::minutes(2));

        automation_manager
            .scheduler_tick_at(&task_manager, now + Duration::milliseconds(50))
            .await?;
        assert_eq!(automation_manager.list_runs(&automation.id, None)?.len(), 1);

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_recovery_consumes_only_one_overdue_slot() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;

        let now = DateTime::parse_from_rfc3339("2026-07-09T09:27:59.900Z")
            .expect("now")
            .with_timezone(&Utc);
        let stale_slot = now - Duration::minutes(10);
        let mut automation = automation_record_with_settings(None, None, None, None);
        automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        automation.next_run_at = Some(stale_slot);
        manager.save_automation(&automation)?;
        manager.save_pending_enqueue(&PendingEnqueueRecord::for_slot(
            &automation.id,
            stale_slot,
            stale_slot,
        ))?;

        manager.scheduler_tick_at(&task_manager, now).await?;

        assert_eq!(
            task_manager.list_tasks(None).await.len(),
            1,
            "recovering one durable overdue slot must be this tick's only catch-up"
        );
        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1);
        assert!(
            manager
                .get_automation(&automation.id)?
                .next_run_at
                .is_some_and(|next| next > now),
            "recovery must advance the cursor past now"
        );

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_recovery_persists_cursor_before_deleting_pending() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let automation_root = tempdir.path().join("automations");
        let manager = AutomationManager::open(automation_root)?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;

        let now = Utc::now();
        let stale_slot = now - Duration::minutes(10);
        let mut automation = automation_record_with_settings(None, None, None, None);
        automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        automation.next_run_at = Some(stale_slot);
        manager.save_automation(&automation)?;
        let pending = PendingEnqueueRecord::for_slot(&automation.id, stale_slot, stale_slot);
        manager.save_pending_enqueue(&pending)?;
        let pending_path = manager.pending_path(&automation.id, &pending.run.id)?;
        manager.fail_next_automation_save();

        manager
            .scheduler_tick(&task_manager)
            .await
            .expect_err("cursor persistence should fail at the injected filesystem obstacle");
        assert!(
            pending_path.exists(),
            "the journal must remain until the recovered run and cursor are both durable"
        );
        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        let task_id = task_manager.list_tasks(None).await[0].id.clone();
        wait_for_terminal_state(&task_manager, &task_id, std::time::Duration::from_secs(10))
            .await?;

        manager.scheduler_tick(&task_manager).await?;

        assert!(!pending_path.exists());
        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1);
        assert!(
            manager
                .get_automation(&automation.id)?
                .next_run_at
                .is_some_and(|next| next > Utc::now())
        );
        assert!(
            manager
                .get_automation(&automation.id)?
                .last_run_at
                .is_some(),
            "scheduled recovery must persist terminal run metadata"
        );

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn manual_run_recovers_journaled_enqueue() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;

        let mut automation = automation_record_with_settings(None, None, None, None);
        automation.status = AutomationStatus::Paused;
        manager.save_automation(&automation)?;

        manager.fail_next_run_save();

        let error = manager
            .run_now(&automation.id, &task_manager)
            .await
            .expect_err("run persistence should fail after the durable task is created");
        assert!(error.to_string().contains("injected run save failure"));
        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert_eq!(
            manager.list_pending_enqueues(&automation.id)?.len(),
            1,
            "run_now must journal before creating its durable task"
        );

        manager.scheduler_tick(&task_manager).await?;

        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1);
        assert!(manager.list_pending_enqueues(&automation.id)?.is_empty());

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn manual_run_invocation_id_is_idempotent() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let invocation_id = Uuid::new_v4().to_string();

        let first = manager
            .run_now_idempotent(&automation.id, &invocation_id, &task_manager)
            .await?;
        let second = manager
            .run_now_idempotent(&automation.id, &invocation_id, &task_manager)
            .await?;

        assert_eq!(first.id, second.id);
        assert_eq!(first.task_id, second.task_id);
        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1);

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_pending_is_quarantined_without_blocking_other_automations() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let now = Utc::now();

        let make_due = || -> Result<AutomationRecord> {
            let mut automation = automation_record_with_settings(None, None, None, None);
            automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
            automation.next_run_at = Some(now - Duration::minutes(1));
            manager.save_automation(&automation)?;
            Ok(automation)
        };
        let corrupt_owner = make_due()?;
        let future_owner = make_due()?;
        let mismatched_owner = make_due()?;
        let healthy = make_due()?;

        let corrupt_path = manager.pending_path(&corrupt_owner.id, "corrupt")?;
        fs::create_dir_all(corrupt_path.parent().expect("pending parent"))?;
        fs::write(&corrupt_path, "{ invalid pending json")?;

        let mut future =
            PendingEnqueueRecord::for_slot(&future_owner.id, now - Duration::minutes(1), now);
        future.schema_version = CURRENT_PENDING_ENQUEUE_SCHEMA_VERSION + 1;
        let future_path = manager.pending_path(&future_owner.id, &future.run.id)?;
        write_json_atomic(&future_path, &future)?;

        let mut mismatched =
            PendingEnqueueRecord::for_slot(&mismatched_owner.id, now - Duration::minutes(1), now);
        mismatched.run.automation_id = "different-automation".to_string();
        let mismatched_path = manager.pending_path(&mismatched_owner.id, &mismatched.run.id)?;
        write_json_atomic(&mismatched_path, &mismatched)?;

        manager.scheduler_tick(&task_manager).await?;

        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert_eq!(manager.list_runs(&healthy.id, None)?.len(), 1);
        for blocked in [&corrupt_owner, &future_owner, &mismatched_owner] {
            assert!(manager.list_runs(&blocked.id, None)?.is_empty());
        }
        assert!(corrupt_path.exists());
        assert!(future_path.exists());
        assert!(mismatched_path.exists());

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn mixed_valid_and_invalid_pending_blocks_owner_before_enqueue() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let now = Utc::now();

        let make_due = || -> Result<AutomationRecord> {
            let mut automation = automation_record_with_settings(None, None, None, None);
            automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
            automation.next_run_at = Some(now - Duration::minutes(1));
            manager.save_automation(&automation)?;
            Ok(automation)
        };
        let mixed_owner = make_due()?;
        let healthy = make_due()?;

        let valid =
            PendingEnqueueRecord::for_slot(&mixed_owner.id, now - Duration::minutes(1), now);
        manager.save_pending_enqueue(&valid)?;
        let valid_path = manager.pending_path(&mixed_owner.id, &valid.run.id)?;
        let pending_dir = manager.pending_dir_for(&mixed_owner.id)?;

        let wrong_stem =
            PendingEnqueueRecord::for_slot(&mixed_owner.id, now - Duration::minutes(2), now);
        let wrong_stem_path = pending_dir.join("wrong-file.json");
        write_json_atomic(&wrong_stem_path, &wrong_stem)?;

        let mut unsafe_id =
            PendingEnqueueRecord::for_slot(&mixed_owner.id, now - Duration::minutes(3), now);
        unsafe_id.run.id = "../escape".to_string();
        let unsafe_id_path = pending_dir.join("unsafe-id.json");
        write_json_atomic(&unsafe_id_path, &unsafe_id)?;

        let mut wrong_kind =
            PendingEnqueueRecord::for_slot(&mixed_owner.id, now - Duration::minutes(4), now);
        wrong_kind.kind = PendingEnqueueKind::Manual;
        let wrong_kind_path = manager.pending_path(&mixed_owner.id, &wrong_kind.run.id)?;
        write_json_atomic(&wrong_kind_path, &wrong_kind)?;

        let mut wrong_key =
            PendingEnqueueRecord::for_slot(&mixed_owner.id, now - Duration::minutes(5), now);
        wrong_key.slot_key = "automation:wrong:slot:0".to_string();
        let wrong_key_path = manager.pending_path(&mixed_owner.id, &wrong_key.run.id)?;
        write_json_atomic(&wrong_key_path, &wrong_key)?;

        manager.scheduler_tick(&task_manager).await?;

        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert_eq!(manager.list_runs(&healthy.id, None)?.len(), 1);
        assert!(manager.list_runs(&mixed_owner.id, None)?.is_empty());
        for path in [
            valid_path,
            wrong_stem_path,
            unsafe_id_path,
            wrong_kind_path,
            wrong_key_path,
        ] {
            assert!(
                path.exists(),
                "pending file must be retained: {}",
                path.display()
            );
        }

        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn forkguard_scheduler_recovers_crash_after_task_enqueue_without_duplicate_task()
    -> Result<()> {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let automation_root = tempdir.path().join("automations");
        let task_root = tempdir.path().join("tasks");
        let manager = AutomationManager::open(automation_root.clone())?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(task_root.clone()),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;

        let now = DateTime::parse_from_rfc3339("2026-07-09T09:27:59.900Z")
            .expect("now")
            .with_timezone(&Utc);
        let mut automation = automation_record_with_settings(None, None, None, None);
        automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        automation.next_run_at = Some(now - Duration::minutes(1));
        manager.save_automation(&automation)?;

        let error = manager
            .scheduler_tick_at_with_failure_after_task_enqueue(&task_manager, now)
            .await
            .expect_err("inject crash between durable task and run write");
        assert!(error.to_string().contains("injected scheduler crash"));
        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert!(manager.list_runs(&automation.id, None)?.is_empty());
        task_manager.shutdown();
        drop(task_manager);
        drop(manager);

        let reopened_tasks = TaskManager::start_with_executor(
            automation_task_config(task_root),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let reopened = AutomationManager::open(automation_root)?;
        reopened
            .scheduler_tick_at(&reopened_tasks, now + Duration::milliseconds(25))
            .await?;

        assert_eq!(
            reopened_tasks.list_tasks(None).await.len(),
            1,
            "slot recovery must reuse the durable task created before the crash"
        );
        let runs = reopened.list_runs(&automation.id, None)?;
        assert_eq!(runs.len(), 1);
        assert!(runs[0].task_id.is_some());
        assert!(
            reopened
                .get_automation(&automation.id)?
                .next_run_at
                .is_some_and(|next| next > now + Duration::milliseconds(25))
        );

        reopened
            .scheduler_tick_at(&reopened_tasks, now + Duration::milliseconds(50))
            .await?;
        assert_eq!(reopened_tasks.list_tasks(None).await.len(), 1);
        assert_eq!(reopened.list_runs(&automation.id, None)?.len(), 1);
        reopened_tasks.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_scans_all_non_terminal_runs_beyond_first_hundred() -> Result<()> {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;

        let mut seed_run = queued_run_for(&automation);
        manager
            .enqueue_run_task(&automation, &mut seed_run, &task_manager)
            .await?;
        let task_id = seed_run.task_id.expect("seed task id");
        let _ =
            wait_for_terminal_state(&task_manager, &task_id, std::time::Duration::from_secs(10))
                .await?;

        let base = Utc::now() - Duration::seconds(2);
        for index in 0..101 {
            let mut run = queued_run_for(&automation);
            run.created_at = base + Duration::milliseconds(index);
            run.scheduled_for = run.created_at;
            run.status = AutomationRunStatus::Running;
            run.task_id = Some(task_id.clone());
            manager.save_run(&run)?;
        }

        manager.reconcile_run_statuses(&task_manager).await?;

        let runs = manager.list_runs(&automation.id, None)?;
        assert_eq!(runs.len(), 101);
        let status_counts = runs.iter().fold(
            std::collections::BTreeMap::<String, usize>::new(),
            |mut counts, run| {
                *counts.entry(format!("{:?}", run.status)).or_default() += 1;
                counts
            },
        );
        assert!(
            runs.iter()
                .all(|run| run.status == AutomationRunStatus::Completed),
            "every non-terminal run must be reconciled, including item 101; statuses: {status_counts:?}"
        );
        task_manager.shutdown();
        Ok(())
    }

    #[test]
    fn limited_listing_does_not_parse_unselected_authority_files() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let base = Utc::now() - Duration::minutes(1);
        let oldest = terminal_run_for(&automation, "run-oldest", base);
        let middle = terminal_run_for(&automation, "run-middle", base + Duration::seconds(1));
        let newest = terminal_run_for(&automation, "run-newest", base + Duration::seconds(2));
        for run in [&oldest, &middle, &newest] {
            manager.save_run(run)?;
        }

        fs::write(
            manager.run_path(&automation.id, &oldest.id)?,
            "{ corrupt but outside the requested limit",
        )?;

        let runs = manager.list_runs(&automation.id, Some(1))?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, newest.id);
        Ok(())
    }

    #[test]
    fn zero_run_limit_reads_no_authority_files() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let run = terminal_run_for(&automation, "run-corrupt", Utc::now());
        manager.save_run(&run)?;
        fs::write(
            manager.run_path(&automation.id, &run.id)?,
            "{ corrupt and must not be parsed",
        )?;

        assert!(manager.list_runs(&automation.id, Some(0))?.is_empty());
        Ok(())
    }

    #[test]
    fn missing_corrupt_and_dirty_run_indexes_rebuild_from_authority() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        let first = terminal_run_for(&automation, "run-first", Utc::now());
        write_json_atomic(&manager.run_path(&automation.id, &first.id)?, &first)?;
        let index_dir = runs_dir.join(".index");
        let index_path = index_dir.join("v1.json");
        let dirty_path = index_dir.join("dirty");

        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1);
        assert!(index_path.is_file(), "a missing index should be rebuilt");

        fs::write(&index_path, "{ corrupt index")?;
        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1);
        serde_json::from_str::<serde_json::Value>(&fs::read_to_string(&index_path)?)?;

        let second = terminal_run_for(
            &automation,
            "run-second",
            first.created_at + Duration::seconds(1),
        );
        write_json_atomic(&manager.run_path(&automation.id, &second.id)?, &second)?;
        fs::write(&dirty_path, "dirty")?;
        let runs = manager.list_runs(&automation.id, None)?;
        assert_eq!(runs.len(), 2);
        assert!(!dirty_path.exists(), "successful rebuild clears dirty");
        Ok(())
    }

    #[test]
    fn equal_timestamp_listing_uses_run_id_as_stable_tiebreaker() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let created_at = Utc::now();
        for id in ["run-z", "run-a", "run-m"] {
            manager.save_run(&terminal_run_for(&automation, id, created_at))?;
        }

        let ids = manager
            .list_runs(&automation.id, None)?
            .into_iter()
            .map(|run| run.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["run-a", "run-m", "run-z"]);
        Ok(())
    }

    #[test]
    fn rebuild_prunes_old_terminal_runs_but_keeps_active_and_pending_runs() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        let base = Utc::now() - Duration::days(2);
        for index in 0..=1_000 {
            let run = terminal_run_for(
                &automation,
                format!("terminal-{index:04}"),
                base + Duration::seconds(index),
            );
            write_json_atomic(&manager.run_path(&automation.id, &run.id)?, &run)?;
        }
        let mut active = queued_run_for(&automation);
        active.id = "active-old".to_string();
        active.created_at = base - Duration::hours(1);
        active.scheduled_for = active.created_at;
        write_json_atomic(&manager.run_path(&automation.id, &active.id)?, &active)?;

        let mut pending = PendingEnqueueRecord::for_slot(
            &automation.id,
            base - Duration::hours(2),
            base - Duration::hours(2),
        );
        pending.run.status = AutomationRunStatus::Completed;
        pending.run.started_at = Some(pending.run.created_at);
        pending.run.ended_at = Some(pending.run.created_at);
        manager.save_pending_enqueue(&pending)?;
        write_json_atomic(
            &manager.run_path(&automation.id, &pending.run.id)?,
            &pending.run,
        )?;

        let runs = manager.list_runs(&automation.id, None)?;
        assert_eq!(
            runs.len(),
            1_002,
            "1000 ordinary terminal + protected pending + active"
        );
        assert!(runs.iter().any(|run| run.id == active.id));
        assert!(runs.iter().any(|run| run.id == pending.run.id));
        assert!(
            !manager.run_path(&automation.id, "terminal-0000")?.exists(),
            "the oldest unprotected terminal run should be pruned"
        );
        Ok(())
    }

    #[test]
    fn invalid_pending_record_blocks_retention_for_its_owner() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        let base = Utc::now() - Duration::days(2);
        for index in 0..=1_000 {
            let run = terminal_run_for(
                &automation,
                format!("terminal-{index:04}"),
                base + Duration::seconds(index),
            );
            write_json_atomic(&manager.run_path(&automation.id, &run.id)?, &run)?;
        }
        let pending_dir = manager.pending_dir_for(&automation.id)?;
        fs::create_dir_all(&pending_dir)?;
        fs::write(pending_dir.join("corrupt.json"), "{ invalid pending")?;

        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 1_001);
        assert!(
            manager.run_path(&automation.id, "terminal-0000")?.exists(),
            "retention must fail closed when pending ownership is uncertain"
        );
        Ok(())
    }

    #[test]
    fn host_guard_protects_old_terminal_run_outside_ordinary_limit() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let protected_run_id = "terminal-protected-old".to_string();
        let manager = AutomationManager::open_with_options(
            tempdir.path().join("automations"),
            AutomationManagerOptions {
                max_unprotected_terminal_runs: 1,
                retention_guard: Some(Arc::new(ProtectNamedRunGuard {
                    protected_run_id: protected_run_id.clone(),
                    fail: false,
                })),
            },
        )?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let base = Utc::now() - Duration::hours(1);
        let runs = [
            terminal_run_for(&automation, &protected_run_id, base),
            terminal_run_for(&automation, "terminal-middle", base + Duration::seconds(1)),
            terminal_run_for(&automation, "terminal-newest", base + Duration::seconds(2)),
        ];
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        for run in &runs {
            write_json_atomic(&manager.run_path(&automation.id, &run.id)?, run)?;
        }

        let retained = manager.list_runs(&automation.id, None)?;

        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].id, "terminal-newest");
        assert_eq!(retained[1].id, protected_run_id);
        assert!(
            !manager
                .run_path(&automation.id, "terminal-middle")?
                .exists()
        );
        Ok(())
    }

    #[test]
    fn host_guard_error_blocks_pruning_for_owner() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open_with_options(
            tempdir.path().join("automations"),
            AutomationManagerOptions {
                max_unprotected_terminal_runs: 1,
                retention_guard: Some(Arc::new(ProtectNamedRunGuard {
                    protected_run_id: String::new(),
                    fail: true,
                })),
            },
        )?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let base = Utc::now() - Duration::hours(1);
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        for index in 0..3 {
            let run = terminal_run_for(
                &automation,
                format!("terminal-{index}"),
                base + Duration::seconds(index),
            );
            write_json_atomic(&manager.run_path(&automation.id, &run.id)?, &run)?;
        }

        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 3);
        Ok(())
    }

    #[test]
    fn retention_guard_does_not_parse_retained_history_on_nonterminal_save() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().join("automations");
        let setup = AutomationManager::open(root.clone())?;
        let automation = automation_record_with_settings(None, None, None, None);
        setup.save_automation(&automation)?;
        let base = Utc::now() - Duration::hours(1);
        for index in 0..2 {
            setup.save_run(&terminal_run_for(
                &automation,
                format!("terminal-{index}"),
                base + Duration::seconds(index),
            ))?;
        }
        drop(setup);
        let manager = AutomationManager::open_with_options(
            root,
            AutomationManagerOptions {
                max_unprotected_terminal_runs: 2,
                retention_guard: Some(Arc::new(ProtectNamedRunGuard {
                    protected_run_id: String::new(),
                    fail: false,
                })),
            },
        )?;
        let mut active = queued_run_for(&automation);
        active.id = "active-run".to_string();
        manager.reset_run_io_probe();

        manager.save_run(&active)?;

        assert_eq!(
            manager.run_authority_read_count(),
            0,
            "a nonterminal save below the retention threshold must not parse terminal history"
        );
        Ok(())
    }

    #[test]
    fn terminal_retention_reads_only_prune_candidates() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root = tempdir.path().join("automations");
        let setup = AutomationManager::open(root.clone())?;
        let automation = automation_record_with_settings(None, None, None, None);
        setup.save_automation(&automation)?;
        let base = Utc::now() - Duration::hours(1);
        for index in 0..4 {
            setup.save_run(&terminal_run_for(
                &automation,
                format!("terminal-{index}"),
                base + Duration::seconds(index),
            ))?;
        }
        drop(setup);
        let manager = AutomationManager::open_with_options(
            root,
            AutomationManagerOptions {
                max_unprotected_terminal_runs: 2,
                retention_guard: Some(Arc::new(ProtectNamedRunGuard {
                    protected_run_id: String::new(),
                    fail: false,
                })),
            },
        )?;
        manager.reset_run_io_probe();

        manager.save_run(&terminal_run_for(
            &automation,
            "terminal-newest",
            base + Duration::seconds(4),
        ))?;

        assert_eq!(
            manager.run_authority_read_count(),
            3,
            "the newest retention floor must not be parsed by the host guard"
        );
        let retained_files = fs::read_dir(manager.runs_dir_for(&automation.id)?)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
            .count();
        assert_eq!(retained_files, 2);
        Ok(())
    }

    #[test]
    fn active_out_of_band_change_rebuilds_clean_index_during_reconciliation() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let mut active = queued_run_for(&automation);
        active.id = "active-out-of-band".to_string();
        manager.save_run(&active)?;
        let ended_at = Utc::now();
        active.status = AutomationRunStatus::Completed;
        active.started_at = Some(active.created_at);
        active.ended_at = Some(ended_at);
        fs::write(
            manager.run_path(&automation.id, &active.id)?,
            serde_json::to_string_pretty(&active)?,
        )?;

        let (runs, latest_terminal_at) = manager.reconciliation_runs(&automation.id)?;

        assert!(runs.is_empty());
        assert_eq!(latest_terminal_at, Some(ended_at));
        Ok(())
    }

    #[test]
    fn clean_index_detects_out_of_band_authority_add_and_remove() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let first = terminal_run_for(&automation, "run-first", Utc::now());
        let second = terminal_run_for(
            &automation,
            "run-second",
            first.created_at + Duration::seconds(1),
        );
        manager.save_run(&first)?;
        write_json_atomic(&manager.run_path(&automation.id, &second.id)?, &second)?;

        let after_add = manager.list_runs(&automation.id, None)?;
        assert_eq!(
            after_add.len(),
            2,
            "an external authority add must invalidate the clean index"
        );

        fs::remove_file(manager.run_path(&automation.id, &first.id)?)?;
        let after_remove = manager.list_runs(&automation.id, None)?;
        assert_eq!(
            after_remove.len(),
            1,
            "an external authority removal must invalidate the clean index"
        );
        assert_eq!(after_remove[0].id, second.id);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_looks_up_only_the_deterministic_slot_file() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let now = Utc::now();
        let mut automation = automation_record_with_settings(None, None, None, None);
        automation.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        automation.next_run_at = Some(now - Duration::minutes(1));
        manager.save_automation(&automation)?;
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        fs::write(runs_dir.join("unrelated-corrupt.json"), "{ corrupt old run")?;

        manager.scheduler_tick(&task_manager).await?;

        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        let saved = fs::read_dir(&runs_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("slot_"))
            .count();
        assert_eq!(saved, 1);
        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_deterministic_slot_fails_closed_without_blocking_other_owners() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let now = DateTime::parse_from_rfc3339("2026-07-09T09:27:59.900Z")
            .expect("now")
            .with_timezone(&Utc);
        let mut blocked = automation_record_with_settings(None, None, None, None);
        blocked.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        blocked.next_run_at = Some(now - Duration::minutes(1));
        manager.save_automation(&blocked)?;
        let mut healthy = automation_record_with_settings(None, None, None, None);
        healthy.rrule = "FREQ=MINUTELY;INTERVAL=1".to_string();
        healthy.next_run_at = Some(now - Duration::minutes(1));
        manager.save_automation(&healthy)?;
        let due_at = AutomationSchedule::parse_rrule(&blocked.rrule)?
            .latest_due_at_or_before(blocked.next_run_at.expect("due"), now)?;
        let target = PendingEnqueueRecord::for_slot(&blocked.id, due_at, now);
        let target_path = manager.run_path(&blocked.id, &target.run.id)?;
        fs::create_dir_all(target_path.parent().expect("run parent"))?;
        fs::write(&target_path, "{ corrupt deterministic slot")?;

        manager.scheduler_tick_at(&task_manager, now).await?;

        assert_eq!(task_manager.list_tasks(None).await.len(), 1);
        assert!(target_path.exists());
        assert_eq!(
            manager.get_automation(&blocked.id)?.next_run_at,
            Some(due_at)
        );
        task_manager.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_reads_active_runs_without_parsing_terminal_history() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let task_manager = TaskManager::start_with_executor(
            automation_task_config(tempdir.path().join("tasks")),
            Arc::new(AutomationNoopExecutor),
        )
        .await?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;

        let mut active = queued_run_for(&automation);
        manager
            .enqueue_run_task(&automation, &mut active, &task_manager)
            .await?;
        let task_id = active.task_id.clone().expect("task id");
        wait_for_terminal_state(&task_manager, &task_id, std::time::Duration::from_secs(10))
            .await?;
        active.status = AutomationRunStatus::Running;
        manager.save_run(&active)?;
        let terminal = terminal_run_for(
            &automation,
            "terminal-corrupt-after-index",
            active.created_at - Duration::seconds(1),
        );
        manager.save_run(&terminal)?;
        fs::write(
            manager.run_path(&automation.id, &terminal.id)?,
            "{ corrupt terminal history",
        )?;

        manager.reconcile_run_statuses(&task_manager).await?;

        assert_eq!(
            manager
                .load_run(&automation.id, &active.id)?
                .expect("active run")
                .status,
            AutomationRunStatus::Completed
        );
        task_manager.shutdown();
        Ok(())
    }

    #[test]
    fn authority_write_survives_index_write_failure_and_dirty_rebuild_recovers() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let first = terminal_run_for(&automation, "run-first", Utc::now());
        manager.save_run(&first)?;
        let index_dir = manager.runs_dir_for(&automation.id)?.join(".index");
        let index_path = index_dir.join("v1.json");
        let dirty_path = index_dir.join("dirty");
        assert!(index_path.is_file());
        fs::remove_file(&index_path)?;
        fs::create_dir(&index_path)?;
        let second = terminal_run_for(
            &automation,
            "run-second",
            first.created_at + Duration::seconds(1),
        );

        manager.save_run(&second)?;

        assert!(manager.run_path(&automation.id, &second.id)?.is_file());
        assert!(dirty_path.exists());
        fs::remove_dir(&index_path)?;
        assert_eq!(manager.list_runs(&automation.id, None)?.len(), 2);
        assert!(!dirty_path.exists());
        Ok(())
    }

    #[test]
    fn dirty_marker_failure_prevents_unjournaled_authority_write() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        fs::create_dir_all(&runs_dir)?;
        fs::write(runs_dir.join(".index"), "blocks the index directory")?;
        let run = terminal_run_for(&automation, "run-unjournaled", Utc::now());

        let result = manager.save_run(&run);

        assert!(
            result.is_err(),
            "authority mutation requires a durable dirty marker"
        );
        assert!(
            !manager.run_path(&automation.id, &run.id)?.exists(),
            "an unjournaled authority record must not be published"
        );
        Ok(())
    }

    #[test]
    fn deleting_automation_removes_run_index_sidecar() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        manager.save_run(&terminal_run_for(&automation, "run", Utc::now()))?;
        let runs_dir = manager.runs_dir_for(&automation.id)?;
        assert!(runs_dir.join(".index").join("v1.json").is_file());

        manager.delete_automation(&automation.id)?;

        assert!(!runs_dir.exists());
        Ok(())
    }

    #[test]
    fn protected_task_ids_cover_retained_runs_and_pending_journals() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;

        let mut retained = terminal_run_for(&automation, "retained-run", Utc::now());
        retained.task_id = Some("retained-task".to_string());
        manager.save_run(&retained)?;

        let mut pending = PendingEnqueueRecord::for_slot(
            &automation.id,
            Utc::now() - Duration::minutes(1),
            Utc::now(),
        );
        pending.run.task_id = Some("pending-task".to_string());
        manager.save_pending_enqueue(&pending)?;

        let protected = manager.protected_task_ids()?;
        assert!(protected.contains("retained-task"));
        assert!(protected.contains("pending-task"));
        Ok(())
    }

    #[test]
    fn invalid_pending_journal_blocks_all_task_pruning() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let manager = AutomationManager::open(tempdir.path().join("automations"))?;
        let automation = automation_record_with_settings(None, None, None, None);
        manager.save_automation(&automation)?;
        let pending_dir = manager.pending_dir_for(&automation.id)?;
        fs::create_dir_all(&pending_dir)?;
        fs::write(pending_dir.join("corrupt.json"), "{ corrupt pending")?;

        let error = manager
            .protected_task_ids()
            .expect_err("invalid pending journal must fail closed");
        assert!(
            error
                .to_string()
                .contains("Task pruning blocked by invalid pending enqueue records")
        );
        Ok(())
    }

    #[test]
    fn default_automations_dir_honors_codewhale_home_as_hard_override() {
        let _lock = crate::test_support::lock_test_env();
        let tmp = tempfile::TempDir::new().unwrap();
        // SAFETY: serialised by lock_test_env.
        unsafe {
            std::env::remove_var("DEEPSEEK_AUTOMATIONS_DIR");
            std::env::set_var("CODEWHALE_HOME", tmp.path());
        }
        // $CODEWHALE_HOME IS the home dir (no ".codewhale" appended); the
        // legacy ~/.deepseek fallback is bypassed entirely.
        assert_eq!(default_automations_dir(), tmp.path().join("automations"));
        // SAFETY: cleanup under the same lock.
        unsafe {
            std::env::remove_var("CODEWHALE_HOME");
        }
    }

    #[test]
    fn default_automations_dir_prefers_deepseek_automations_dir_over_codewhale_home() {
        let _lock = crate::test_support::lock_test_env();
        let tmp = tempfile::TempDir::new().unwrap();
        // SAFETY: serialised by lock_test_env.
        unsafe {
            std::env::set_var("DEEPSEEK_AUTOMATIONS_DIR", tmp.path());
            std::env::set_var("CODEWHALE_HOME", "/should/not/be/used");
        }
        // The most-specific override wins over the base-data-dir override.
        assert_eq!(default_automations_dir(), tmp.path());
        // SAFETY: cleanup under the same lock.
        unsafe {
            std::env::remove_var("DEEPSEEK_AUTOMATIONS_DIR");
            std::env::remove_var("CODEWHALE_HOME");
        }
    }
}
